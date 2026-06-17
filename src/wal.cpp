// ─────────────────────────────────────────────────────────────────────────────
// wal.cpp — Write-Ahead Log implementation
//
// Implementation notes:
//   - CRC32 lookup table is constexpr-computed at compile time (zero runtime cost)
//   - pwrite() is used for atomic positional writes (no seek + write race)
//   - pread() is used for positional reads during replay (no seek state)
//   - fdatasync() flushes data to disk without flushing unnecessary metadata
//   - write_mutex_ serializes appends for future multi-threaded use
// ─────────────────────────────────────────────────────────────────────────────

#include "wal.h"

#include <array>          // std::array for CRC32 table
#include <cassert>        // assert
#include <cerrno>         // errno
#include <cstdio>         // fprintf, stderr
#include <cstring>        // strerror, memcpy
#include <fcntl.h>        // open, O_RDWR, O_CREAT, O_CLOEXEC
#include <sys/stat.h>     // fstat
#include <unistd.h>       // pwrite, pread, fdatasync, close, ftruncate
#include <vector>         // std::vector for replay buffer

// ─────────────────────────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────────────────────────

#define WAL_LOG_ERR(fmt, ...) \
    fprintf(stderr, "[WAL] ERROR: " fmt " (errno=%d: %s)\n", \
            ##__VA_ARGS__, errno, strerror(errno))

#define WAL_LOG_INFO(fmt, ...) \
    fprintf(stderr, "[WAL] INFO: " fmt "\n", ##__VA_ARGS__)

// ─────────────────────────────────────────────────────────────────────────────
// CRC32 — Compile-time lookup table generation
// ─────────────────────────────────────────────────────────────────────────────
// CRC32 uses polynomial 0xEDB88320 (bit-reversed form of 0x04C11DB7).
// This is the standard polynomial used by Ethernet, gzip, PNG, and zlib.
//
// The lookup table maps each byte value to its CRC contribution, allowing
// us to process one byte per iteration instead of one bit. This turns
// CRC32 from O(8n) to O(n) with a 1KB lookup table.
//
// constexpr computation: The table is fully computed at compile time in C++20.
// The compiler generates a static array in the binary — zero runtime init.

namespace {

constexpr std::array<uint32_t, 256> make_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            // If the low bit is set, XOR with the polynomial.
            // Otherwise, just shift right. This is the standard
            // bit-by-bit CRC division algorithm.
            crc = (crc & 1u) ? ((crc >> 1) ^ 0xEDB88320u) : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCrc32Table = make_crc32_table();

}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// compute_crc32 — Calculate CRC32 checksum
// ─────────────────────────────────────────────────────────────────────────────

uint32_t WAL::compute_crc32(const uint8_t* data, std::size_t len) {
    // Start with all 1s (standard CRC32 initialization).
    // The inversion ensures that leading zero bytes affect the CRC
    // (without it, CRC of "\x00\x00hello" == CRC of "hello").
    uint32_t crc = 0xFFFFFFFF;

    for (std::size_t i = 0; i < len; ++i) {
        // XOR the next byte into the low 8 bits of CRC, look up the
        // table entry, and XOR with the shifted CRC. This processes
        // 8 bits (one byte) per iteration.
        crc = kCrc32Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }

    // Final inversion (standard CRC32 finalization).
    return crc ^ 0xFFFFFFFF;
}

// ─────────────────────────────────────────────────────────────────────────────
// Constructor / Destructor
// ─────────────────────────────────────────────────────────────────────────────

WAL::WAL(const std::string& path, SyncPolicy policy, uint32_t sync_interval_ms)
    : path_(path)
    , policy_(policy)
    , sync_interval_ms_(sync_interval_ms) {
}

WAL::~WAL() {
    // Stop periodic sync thread before closing the fd
    if (sync_running_.load()) {
        sync_running_.store(false);
        sync_cv_.notify_one();
        if (sync_thread_.joinable()) {
            sync_thread_.join();
        }
    }

    // Final sync + close
    if (fd_ != -1) {
        // One last fdatasync to flush any pending writes.
        // This is important for kPeriodic policy: the last few writes
        // before shutdown might not have been synced yet.
        fdatasync(fd_);
        close(fd_);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// open — Create or open the WAL file
// ─────────────────────────────────────────────────────────────────────────────

std::string WAL::open() {
    // O_RDWR:    We need to read (replay) and write (append).
    // O_CREAT:   Create the file if it doesn't exist.
    // O_CLOEXEC: Close on exec to prevent fd leaks.
    // 0644:      Owner read/write, group/other read-only.
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd_ == -1) {
        WAL_LOG_ERR("Failed to open WAL file: %s", path_.c_str());
        return "Failed to open WAL file: " + path_;
    }

    // Determine the current file size to set the write position.
    // After replay(), write_pos_ will be adjusted to the end of the
    // last valid entry (which may be less than file size if the file
    // contains trailing garbage from a crash).
    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        WAL_LOG_ERR("Failed to stat WAL file: %s", path_.c_str());
        return "Failed to stat WAL file";
    }
    write_pos_ = st.st_size;

    // Start the periodic sync thread if requested
    if (policy_ == SyncPolicy::kPeriodic) {
        sync_running_.store(true);
        sync_thread_ = std::thread(&WAL::sync_thread_func, this);
    }

    WAL_LOG_INFO("Opened WAL: %s (size=%ld, policy=%s)",
                 path_.c_str(),
                 static_cast<long>(st.st_size),
                 policy_ == SyncPolicy::kEveryWrite ? "every_write" :
                 policy_ == SyncPolicy::kPeriodic   ? "periodic" : "none");

    return "";  // Success
}

// ─────────────────────────────────────────────────────────────────────────────
// append_set / append_del — Public append API
// ─────────────────────────────────────────────────────────────────────────────

bool WAL::append_set(std::string_view key, std::string_view value,
                     std::vector<uint8_t>* out_bytes) {
    return append_entry(Entry::OpType::kSet, key, value, out_bytes);
}

bool WAL::append_del(std::string_view key,
                     std::vector<uint8_t>* out_bytes) {
    // DEL entries have an empty value
    return append_entry(Entry::OpType::kDel, key, "", out_bytes);
}

// ─────────────────────────────────────────────────────────────────────────────
// append_entry — Serialize and write a WAL entry
// ─────────────────────────────────────────────────────────────────────────────

bool WAL::append_entry(Entry::OpType op, std::string_view key,
                       std::string_view value, std::vector<uint8_t>* out_bytes) {
    // ── Validate key/value sizes ────────────────────────────────────────
    if (key.size() > wal_format::kMaxKeyLen) {
        WAL_LOG_ERR("Key too large: %zu bytes (max %u)",
                    key.size(), wal_format::kMaxKeyLen);
        return false;
    }
    if (value.size() > wal_format::kMaxValLen) {
        WAL_LOG_ERR("Value too large: %zu bytes (max %u)",
                    value.size(), wal_format::kMaxValLen);
        return false;
    }

    // ── Calculate sizes ─────────────────────────────────────────────────
    // payload = timestamp + op_type + key_len + key + val_len + value
    uint32_t payload_size = wal_format::kTimestampSize +
                            wal_format::kOpTypeSize +
                            wal_format::kKeyLenSize +
                            static_cast<uint32_t>(key.size()) +
                            wal_format::kValLenSize +
                            static_cast<uint32_t>(value.size());

    // entry_len = payload + crc32 (this is what we write as the length header)
    uint32_t entry_len = payload_size + wal_format::kCrc32Size;

    // total_size = entry_len_field + entry_len
    uint32_t total_size = wal_format::kEntryLenSize + entry_len;

    // ── Serialize into buffer ───────────────────────────────────────────
    // Stack-allocate for entries up to 4KB (covers 99.9% of real KV entries).
    // Heap-allocate only for unusually large values.
    uint8_t stack_buf[4096];
    std::vector<uint8_t> heap_buf;
    uint8_t* buf;

    if (total_size <= sizeof(stack_buf)) {
        buf = stack_buf;
    } else {
        heap_buf.resize(total_size);
        buf = heap_buf.data();
    }

    uint8_t* p = buf;

    // Write entry_len (4 bytes, little-endian)
    memcpy(p, &entry_len, 4);
    p += 4;

    // ── Payload starts here (this is what CRC32 covers) ─────────────────
    uint8_t* payload_start = p;

    // Timestamp: nanoseconds since Unix epoch
    // Using system_clock (not steady_clock) because:
    //   - WAL entries need a meaningful wall-clock time for debugging
    //   - steady_clock values are meaningless across restarts
    auto now = std::chrono::system_clock::now();
    uint64_t ts = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count());
    memcpy(p, &ts, 8);
    p += 8;

    // Op type
    *p++ = static_cast<uint8_t>(op);

    // Key length + key data
    uint16_t klen = static_cast<uint16_t>(key.size());
    memcpy(p, &klen, 2);
    p += 2;
    memcpy(p, key.data(), key.size());
    p += key.size();

    // Value length + value data
    uint16_t vlen = static_cast<uint16_t>(value.size());
    memcpy(p, &vlen, 2);
    p += 2;
    memcpy(p, value.data(), value.size());
    p += value.size();

    // ── CRC32 over payload ──────────────────────────────────────────────
    uint32_t crc = compute_crc32(payload_start, payload_size);
    memcpy(p, &crc, 4);
    p += 4;

    // Sanity check: we should have written exactly total_size bytes
    // This assert catches serialization bugs during development.
    assert(static_cast<uint32_t>(p - buf) == total_size);

    // ── Capture serialized bytes for replication ────────────────────────
    // If the caller wants the raw bytes (for broadcasting to replicas),
    // copy them now — before we acquire the write mutex, to minimize
    // time spent under the lock.
    if (out_bytes != nullptr) {
        out_bytes->assign(buf, buf + total_size);
    }

    // ── Write to file ───────────────────────────────────────────────────
    // Lock the write mutex to serialize concurrent appends.
    // pwrite() writes at a specific file offset without changing the
    // file's current position. This is atomic: no other thread can
    // interleave a seek between our seek and write.
    std::lock_guard lock(write_mutex_);

    ssize_t written = pwrite(fd_, buf, total_size, write_pos_);
    if (written != static_cast<ssize_t>(total_size)) {
        WAL_LOG_ERR("pwrite failed: wrote %zd of %u bytes at offset %ld",
                    written, total_size, static_cast<long>(write_pos_));
        return false;
    }

    write_pos_ += total_size;

    // ── Sync based on policy ────────────────────────────────────────────
    // fdatasync: flushes file data to the physical disk.
    //   - Does NOT flush unnecessary metadata (timestamps, permissions)
    //   - Faster than fsync() which flushes all metadata
    //   - On Linux with ext4, fdatasync skips the journal if only data changed
    if (policy_ == SyncPolicy::kEveryWrite) {
        if (fdatasync(fd_) == -1) {
            WAL_LOG_ERR("fdatasync failed");
            return false;
        }
    }

    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// replay — Read and validate all WAL entries
// ─────────────────────────────────────────────────────────────────────────────

std::size_t WAL::replay(const std::function<void(const Entry&)>& callback) {
    if (fd_ == -1) return 0;

    // Get current file size
    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        WAL_LOG_ERR("Failed to stat WAL file during replay");
        return 0;
    }
    off_t file_size = st.st_size;

    if (file_size == 0) {
        write_pos_ = 0;
        return 0;  // Empty WAL, nothing to replay
    }

    off_t pos = 0;
    std::size_t count = 0;

    while (pos + static_cast<off_t>(wal_format::kEntryLenSize) <= file_size) {
        // ── Read entry_len ──────────────────────────────────────────────
        uint32_t entry_len = 0;
        ssize_t n = pread(fd_, &entry_len, 4, pos);
        if (n != 4) {
            WAL_LOG_INFO("Partial entry_len at offset %ld — stopping replay",
                         static_cast<long>(pos));
            break;
        }

        // Validate entry_len
        if (entry_len < wal_format::kMinEntryLen) {
            WAL_LOG_INFO("Invalid entry_len %u at offset %ld — stopping replay",
                         entry_len, static_cast<long>(pos));
            break;
        }

        // Check if the full entry fits in the file
        if (pos + 4 + static_cast<off_t>(entry_len) > file_size) {
            WAL_LOG_INFO("Incomplete entry at offset %ld (need %u bytes, "
                         "have %ld) — stopping replay",
                         static_cast<long>(pos), entry_len,
                         static_cast<long>(file_size - pos - 4));
            break;
        }

        // ── Read entry data ─────────────────────────────────────────────
        std::vector<uint8_t> data(entry_len);
        n = pread(fd_, data.data(), entry_len, pos + 4);
        if (n != static_cast<ssize_t>(entry_len)) {
            WAL_LOG_INFO("Short read at offset %ld — stopping replay",
                         static_cast<long>(pos));
            break;
        }

        // ── Verify CRC32 ────────────────────────────────────────────────
        // CRC32 is the last 4 bytes of the entry data.
        // Payload is everything before the CRC32.
        uint32_t payload_size = entry_len - wal_format::kCrc32Size;
        uint32_t stored_crc = 0;
        memcpy(&stored_crc, data.data() + payload_size, 4);

        uint32_t computed_crc = compute_crc32(data.data(), payload_size);

        if (stored_crc != computed_crc) {
            WAL_LOG_INFO("CRC32 mismatch at offset %ld: stored=0x%08X "
                         "computed=0x%08X — stopping replay",
                         static_cast<long>(pos), stored_crc, computed_crc);
            break;
        }

        // ── Parse entry ─────────────────────────────────────────────────
        const uint8_t* p = data.data();

        // Timestamp
        uint64_t ts = 0;
        memcpy(&ts, p, 8);
        p += 8;

        // Op type
        uint8_t op_byte = *p++;
        if (op_byte != static_cast<uint8_t>(Entry::OpType::kSet) &&
            op_byte != static_cast<uint8_t>(Entry::OpType::kDel)) {
            WAL_LOG_INFO("Unknown op_type %u at offset %ld — stopping replay",
                         op_byte, static_cast<long>(pos));
            break;
        }

        // Key
        uint16_t key_len = 0;
        memcpy(&key_len, p, 2);
        p += 2;

        // Validate that key data fits within the entry
        if (p + key_len > data.data() + payload_size) {
            WAL_LOG_INFO("Key length %u exceeds entry at offset %ld — stopping replay",
                         key_len, static_cast<long>(pos));
            break;
        }
        std::string key(reinterpret_cast<const char*>(p), key_len);
        p += key_len;

        // Value
        uint16_t val_len = 0;
        memcpy(&val_len, p, 2);
        p += 2;

        if (p + val_len > data.data() + payload_size) {
            WAL_LOG_INFO("Value length %u exceeds entry at offset %ld — stopping replay",
                         val_len, static_cast<long>(pos));
            break;
        }
        std::string value(reinterpret_cast<const char*>(p), val_len);

        // ── Invoke callback ─────────────────────────────────────────────
        Entry entry;
        entry.op = static_cast<Entry::OpType>(op_byte);
        entry.key = std::move(key);
        entry.value = std::move(value);
        entry.timestamp_ns = ts;

        callback(entry);
        ++count;

        pos += 4 + entry_len;
    }

    // ── Truncate trailing garbage ───────────────────────────────────────
    // If the file has data beyond the last valid entry (from a crash
    // mid-write), truncate it. This ensures the next append starts
    // immediately after the last valid entry, and future replays
    // don't encounter the garbage again.
    if (pos < file_size) {
        WAL_LOG_INFO("Truncating WAL from %ld to %ld bytes (discarding %ld bytes of garbage)",
                     static_cast<long>(file_size), static_cast<long>(pos),
                     static_cast<long>(file_size - pos));
        if (ftruncate(fd_, pos) == -1) {
            WAL_LOG_ERR("Failed to truncate WAL after replay");
        }
    }

    write_pos_ = pos;

    WAL_LOG_INFO("Replayed %zu entries (%ld bytes)", count, static_cast<long>(pos));
    return count;
}

// ─────────────────────────────────────────────────────────────────────────────
// truncate — Discard all WAL entries
// ─────────────────────────────────────────────────────────────────────────────

bool WAL::truncate() {
    std::lock_guard lock(write_mutex_);

    if (ftruncate(fd_, 0) == -1) {
        WAL_LOG_ERR("Failed to truncate WAL");
        return false;
    }

    write_pos_ = 0;
    WAL_LOG_INFO("WAL truncated");
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// sync_thread_func — Background periodic fdatasync
// ─────────────────────────────────────────────────────────────────────────────

void WAL::sync_thread_func() {
    while (sync_running_.load()) {
        // Wait for the sync interval or until notified to stop.
        // condition_variable::wait_for releases the lock while waiting,
        // allowing the main thread to continue appending.
        std::unique_lock lock(sync_cv_mutex_);
        sync_cv_.wait_for(lock, std::chrono::milliseconds(sync_interval_ms_));

        // After waking up (timeout or notify), sync if still running
        if (sync_running_.load() && fd_ != -1) {
            fdatasync(fd_);
        }
    }
}
