#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// wal.h — Write-Ahead Log for crash recovery
//
// Design:
//   The WAL is an append-only binary file that records every mutating
//   operation (SET, DEL) before it modifies the in-memory store. On crash,
//   the store is rebuilt by replaying the WAL from the beginning.
//
//   This guarantees durability: if the server ACKs a write to the client,
//   the write is on disk (or will be, depending on sync policy).
//
// Binary format (little-endian, x86_64 only):
//   ┌──────────┬───────────┬────────┬─────────┬──────┬─────────┬───────┬───────┐
//   │entry_len │ timestamp │op_type │ key_len │ key  │ val_len │ value │ crc32 │
//   │  4 bytes │  8 bytes  │1 byte  │ 2 bytes │ var  │ 2 bytes │  var  │4 bytes│
//   └──────────┴───────────┴────────┴─────────┴──────┴─────────┴───────┴───────┘
//
//   entry_len: number of bytes AFTER this field (payload + crc32)
//   timestamp: nanoseconds since Unix epoch (system_clock for wall-clock debugging)
//   op_type:   1 = SET, 2 = DEL
//   key_len:   key length in bytes (max 65535)
//   key:       raw key bytes
//   val_len:   value length in bytes (0 for DEL)
//   value:     raw value bytes (empty for DEL)
//   crc32:     CRC32 of [timestamp..value] (payload only, not entry_len or crc32)
//
// Recovery semantics:
//   On replay, entries are read sequentially. If an entry has a CRC32
//   mismatch or is incomplete (partial write from a crash), replay stops.
//   The WAL is truncated to the last valid entry to discard garbage.
//   This provides crash consistency: either an entry is fully written
//   and valid, or it's discarded.
//
// Sync policies:
//   kEveryWrite: fdatasync() after every append (safest, ~500 μs overhead)
//   kPeriodic:   fdatasync() every N ms via background thread (balance)
//   kNone:       No explicit sync — OS flushes when it wants (fastest, for tests)
// ─────────────────────────────────────────────────────────────────────────────

#include <atomic>               // std::atomic for sync thread control
#include <bit>                  // std::endian (C++20)
#include <chrono>               // std::chrono::system_clock
#include <condition_variable>   // std::condition_variable for periodic sync
#include <cstddef>              // std::size_t
#include <cstdint>              // uint8_t, uint16_t, uint32_t, uint64_t
#include <functional>           // std::function for replay callback
#include <mutex>                // std::mutex for write serialization
#include <string>               // std::string
#include <string_view>          // std::string_view for zero-copy API
#include <thread>               // std::thread for periodic sync
#include <vector>               // std::vector for out_bytes

// ── Endianness check ────────────────────────────────────────────────────────
// Our binary WAL format uses native byte order (little-endian on x86_64).
// This static_assert fails at compile time on big-endian platforms,
// preventing silent data corruption.
static_assert(std::endian::native == std::endian::little,
    "WAL binary format assumes little-endian byte order (x86_64)");

// ─────────────────────────────────────────────────────────────────────────────
// Sync Policy
// ─────────────────────────────────────────────────────────────────────────────

enum class SyncPolicy : uint8_t {
    kEveryWrite,  // fdatasync after every append — maximum durability
    kPeriodic,    // fdatasync every sync_interval_ms — durability/perf balance
    kNone         // No explicit sync — fastest, for unit tests only
};

// ─────────────────────────────────────────────────────────────────────────────
// WAL Format Constants
// ─────────────────────────────────────────────────────────────────────────────
// Centralizing field sizes prevents magic numbers and makes the format
// self-documenting. Any change to the format is immediately visible here.

namespace wal_format {
    // Field sizes in bytes
    static constexpr uint32_t kEntryLenSize   = 4;  // uint32_t
    static constexpr uint32_t kTimestampSize  = 8;  // uint64_t
    static constexpr uint32_t kOpTypeSize     = 1;  // uint8_t
    static constexpr uint32_t kKeyLenSize     = 2;  // uint16_t
    static constexpr uint32_t kValLenSize     = 2;  // uint16_t
    static constexpr uint32_t kCrc32Size      = 4;  // uint32_t

    // Fixed overhead per entry (everything except key/value data)
    //   timestamp(8) + op_type(1) + key_len(2) + val_len(2) + crc32(4) = 17
    static constexpr uint32_t kFixedPayloadOverhead =
        kTimestampSize + kOpTypeSize + kKeyLenSize + kValLenSize + kCrc32Size;

    // Minimum entry_len: a DEL with 0-byte key has kFixedPayloadOverhead bytes
    static constexpr uint32_t kMinEntryLen = kFixedPayloadOverhead;

    // Maximum key/value sizes (limited by uint16_t)
    static constexpr uint16_t kMaxKeyLen = 65535;
    static constexpr uint16_t kMaxValLen = 65535;
}

// ─────────────────────────────────────────────────────────────────────────────
// WAL — Write-Ahead Log
// ─────────────────────────────────────────────────────────────────────────────

class WAL {
public:
    // ── Entry — a single WAL record as seen during replay ───────────────
    struct Entry {
        enum class OpType : uint8_t {
            kSet = 1,
            kDel = 2
        };

        OpType op;
        std::string key;
        std::string value;        // Empty for DEL
        uint64_t timestamp_ns;    // Nanoseconds since Unix epoch
    };

    // ── Constructor / Destructor ────────────────────────────────────────
    // Two-phase init: construct, then call open().
    // This avoids throwing from the constructor (same pattern as Server).
    //
    // sync_interval_ms: only used when policy == kPeriodic
    explicit WAL(const std::string& path,
                 SyncPolicy policy = SyncPolicy::kEveryWrite,
                 uint32_t sync_interval_ms = 100);
    ~WAL();

    // Non-copyable, non-movable (owns file descriptor and thread)
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&&) = delete;
    WAL& operator=(WAL&&) = delete;

    // ── Open / Create the WAL file ──────────────────────────────────────
    // Creates the file if it doesn't exist, opens it if it does.
    // Returns empty string on success, error message on failure.
    [[nodiscard]] std::string open();

    // ── Append operations ───────────────────────────────────────────────
    // Write a SET or DEL entry to the WAL.
    // Thread-safe: serialized by an internal mutex.
    //
    // Returns true on success. On failure (disk full, I/O error), returns
    // false. The caller should NOT update the in-memory store if the WAL
    // write fails — this maintains the WAL-first invariant.
    //
    // out_bytes (optional): If non-null, the serialized entry bytes
    // (entry_len + payload + crc32) are copied into this vector.
    // Used by the replication manager to broadcast entries to replicas
    // without re-serializing.
    bool append_set(std::string_view key, std::string_view value,
                    std::vector<uint8_t>* out_bytes = nullptr);
    bool append_del(std::string_view key,
                    std::vector<uint8_t>* out_bytes = nullptr);

    // ── Replay ──────────────────────────────────────────────────────────
    // Read all valid entries from the WAL file and invoke the callback
    // for each one. Stops at the first corrupted or incomplete entry.
    // After replay, truncates the file to discard any trailing garbage.
    //
    // Returns the number of entries successfully replayed.
    //
    // The callback receives each entry in order. Typical usage:
    //   wal.replay([&store](const WAL::Entry& e) {
    //       if (e.op == Entry::OpType::kSet) store.set(e.key, e.value);
    //       else store.del(e.key);
    //   });
    std::size_t replay(const std::function<void(const Entry&)>& callback);

    // ── Truncate ────────────────────────────────────────────────────────
    // Discard all WAL entries (e.g., after taking a snapshot).
    // Resets the write position to 0.
    bool truncate();

    // ── CRC32 — public for testability ──────────────────────────────────
    // Computes CRC32 using the standard polynomial 0xEDB88320 (same as
    // zlib, gzip, PNG). The lookup table is constexpr-computed at compile
    // time — zero runtime initialization cost.
    static uint32_t compute_crc32(const uint8_t* data, std::size_t len);

    // ── Accessors (for testing) ─────────────────────────────────────────
    [[nodiscard]] off_t write_position() const { return write_pos_; }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    // ── File state ──────────────────────────────────────────────────────
    std::string path_;
    int fd_ = -1;
    off_t write_pos_ = 0;

    // ── Sync configuration ──────────────────────────────────────────────
    SyncPolicy policy_;
    uint32_t sync_interval_ms_;

    // ── Write serialization ─────────────────────────────────────────────
    // Mutex serializes concurrent appends. Even though the server is
    // currently single-threaded, this prepares for Layer 5 (replication)
    // where the replication thread might write to the WAL concurrently.
    std::mutex write_mutex_;

    // ── Periodic sync thread ────────────────────────────────────────────
    std::thread sync_thread_;
    std::atomic<bool> sync_running_{false};
    std::mutex sync_cv_mutex_;
    std::condition_variable sync_cv_;

    // ── Internal methods ────────────────────────────────────────────────
    bool append_entry(Entry::OpType op, std::string_view key,
                      std::string_view value, std::vector<uint8_t>* out_bytes);
    void sync_thread_func();
};
