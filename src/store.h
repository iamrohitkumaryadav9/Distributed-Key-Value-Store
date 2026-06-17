#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// store.h — Sharded concurrent hash map with TTL support
//
// Design:
//   The store is partitioned into NumShards independent shards, each with
//   its own hash map and shared_mutex. This design minimizes lock contention
//   because operations on different shards never contend — a reader on
//   shard 0 doesn't block a writer on shard 7.
//
//   Shard selection uses bitwise AND with a power-of-2 shard count:
//     shard_index = hash(key) & (NUM_SHARDS - 1)
//   This is faster than modulo (%) because:
//     - AND is 1 CPU cycle, DIV is 20-40 cycles on x86
//     - The compiler can't optimize arbitrary modulo to AND unless it
//       can prove the divisor is a power of 2 at compile time
//     - Our static_assert guarantees the power-of-2 constraint
//
//   Each shard struct is aligned to a 64-byte cache line boundary using
//   alignas(64) to prevent false sharing — see Decision 3 in DESIGN_LAYER3.md.
//
// Locking strategy:
//   GET:       std::shared_lock  (concurrent reads allowed)
//   SET / DEL: std::unique_lock  (exclusive write access)
//
// TTL:
//   Each entry optionally stores an expiry timestamp using steady_clock.
//   Expiry is checked lazily on GET: if a key is expired, the GET returns
//   nullopt and the entry is deleted (requires lock escalation from shared
//   to unique). This is the same strategy Redis uses.
// ─────────────────────────────────────────────────────────────────────────────

#include <array>            // std::array for shard storage
#include <chrono>           // std::chrono::steady_clock for TTL
#include <cstddef>          // std::size_t
#include <functional>       // std::hash
#include <optional>         // std::optional for get() return and TTL
#include <shared_mutex>     // std::shared_mutex for reader-writer locking
#include <string>           // std::string for key/value storage
#include <string_view>      // std::string_view for zero-copy lookups
#include <unordered_map>    // std::unordered_map for per-shard storage

// ─────────────────────────────────────────────────────────────────────────────
// Transparent Hash + Equality for zero-allocation lookups
// ─────────────────────────────────────────────────────────────────────────────
// These enable std::unordered_map::find(std::string_view) without creating
// a temporary std::string. The `is_transparent` tag (C++20, P0919R3) tells
// the container to accept any hashable/comparable type for lookups.
//
// Without this, every find(string_view) would construct a temporary
// std::string — one heap allocation per lookup on the read hot path.

struct TransparentStringHash {
    using is_transparent = void;

    std::size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
};

struct TransparentStringEqual {
    using is_transparent = void;

    bool operator()(std::string_view a, std::string_view b) const noexcept {
        return a == b;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Entry — A value with optional TTL
// ─────────────────────────────────────────────────────────────────────────────
// Each key maps to an Entry, not a raw string. The optional expiry field
// enables TTL without paying storage overhead for non-expiring keys
// (std::optional<time_point> is nullopt when TTL is not set).
//
// Why steady_clock (not system_clock)?
//   steady_clock is monotonic — it never jumps forward or backward.
//   system_clock can be adjusted by NTP, daylight saving, or manual
//   intervention. If the clock jumps backward, keys would appear to
//   "un-expire." steady_clock guarantees forward progress.

struct Entry {
    std::string value;
    std::optional<std::chrono::steady_clock::time_point> expiry;

    // Check if this entry has expired.
    // Called on every GET to implement lazy expiration.
    [[nodiscard]] bool is_expired() const noexcept {
        return expiry.has_value() &&
               std::chrono::steady_clock::now() >= expiry.value();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Store — Sharded concurrent hash map
// ─────────────────────────────────────────────────────────────────────────────
// Template parameter NumShards allows compile-time configuration.
// Default is 16 shards — enough to eliminate contention with up to ~16
// concurrent threads. The benchmark uses Store<1> vs Store<16> to
// demonstrate the sharding benefit.

template<std::size_t NumShards = 16>
class Store {
public:
    static constexpr std::size_t NUM_SHARDS = NumShards;

    // ── Compile-time constraints ────────────────────────────────────────
    // Power-of-2 requirement: shard_index uses bitwise AND, which only
    // works correctly as a modulo replacement when the divisor is 2^k.
    //   hash & (N-1) == hash % N   iff N is a power of 2
    // If N is not a power of 2, AND produces a biased distribution.
    static_assert(NUM_SHARDS > 0,
        "Must have at least one shard");
    static_assert((NUM_SHARDS & (NUM_SHARDS - 1)) == 0,
        "NUM_SHARDS must be a power of 2 for correct bitwise AND sharding");

    // ── Public API ──────────────────────────────────────────────────────

    // Look up a key. Returns the value if found and not expired, nullopt otherwise.
    // Takes a shared lock (allows concurrent reads on the same shard).
    // If the key is expired, escalates to a unique lock to lazily delete it.
    //
    // Returns std::optional<std::string> (a copy of the value) because:
    //   - Returning a pointer/reference would be unsafe: the lock is released
    //     when this function returns, so the reference could dangle.
    //   - The copy happens while the shared lock is held, guaranteeing the
    //     value is consistent.
    //   - For small values (< 64 bytes, typical for KV stores), the copy is
    //     a single memcpy into a pre-allocated SSO buffer (no heap allocation
    //     for strings ≤ 15 chars on libstdc++).
    std::optional<std::string> get(std::string_view key);

    // Insert or update a key-value pair (no TTL — lives forever).
    // Takes a unique lock (exclusive access to the shard).
    void set(std::string_view key, std::string_view value);

    // Insert or update a key-value pair with a TTL.
    // The key expires after `ttl` milliseconds from now.
    void set_with_ttl(std::string_view key, std::string_view value,
                      std::chrono::milliseconds ttl);

    // Delete a key. Returns true if the key existed, false otherwise.
    // Takes a unique lock.
    bool del(std::string_view key);

private:
    // ── Shard structure ─────────────────────────────────────────────────
    // Each shard is a self-contained unit: its own hash map and its own mutex.
    //
    // alignas(64): Forces each Shard to start on a 64-byte cache line boundary.
    //
    // WHY THIS MATTERS — FALSE SHARING:
    //   Modern CPUs operate on 64-byte cache lines, not individual bytes.
    //   If two Shard structs share a cache line, and Thread A writes to
    //   Shard[0].mutex while Thread B reads Shard[1].data, the CPU must
    //   invalidate B's cache line even though the data B is reading hasn't
    //   changed. This is "false sharing" — contention caused by proximity
    //   in memory, not by actual data dependencies.
    //
    //   False sharing can degrade multi-threaded performance by 10-50x.
    //   alignas(64) ensures each shard occupies its own set of cache lines,
    //   eliminating cross-shard false sharing entirely.
    //
    //   The cost is wasted padding: if a Shard is 100 bytes, alignas(64)
    //   rounds it to 128 bytes (2 cache lines). With 16 shards, that's
    //   at most ~450 bytes of padding — negligible for the performance gain.
    struct alignas(64) Shard {
        // The hash map for this shard. Using transparent hash/equal for
        // zero-allocation string_view lookups.
        std::unordered_map<
            std::string,
            Entry,
            TransparentStringHash,
            TransparentStringEqual
        > data;

        // Reader-writer mutex for this shard.
        // mutable: allows locking in const methods (get is logically const
        // even though it acquires a lock).
        //
        // Why shared_mutex (not regular mutex)?
        //   GET (read) operations are typically 10-100x more frequent than
        //   SET/DEL (write) operations in most KV workloads. shared_mutex
        //   allows concurrent GETs on the same shard — N readers can proceed
        //   in parallel, only blocking when a writer arrives.
        //
        // Alternative: std::mutex + double-buffering (read from buffer A,
        //   write to buffer B, then swap). Lower latency for reads but
        //   2x memory usage. shared_mutex is the standard approach.
        mutable std::shared_mutex mutex;
    };

    // Verify that Shard alignment is what we requested.
    static_assert(alignof(Shard) == 64,
        "Shard must be 64-byte aligned to prevent false sharing");

    // ── Shard array ─────────────────────────────────────────────────────
    // std::array is used instead of C-style array for:
    //   1. .size() method (self-documenting)
    //   2. Bounds checking in debug builds (operator[] vs at())
    //   3. Copy/move semantics (array is a value type)
    std::array<Shard, NUM_SHARDS> shards_;

    // ── Shard selection ─────────────────────────────────────────────────
    // Maps a key to its shard index using bitwise AND.
    //
    // Why bitwise AND instead of modulo (%)?
    //   For a power-of-2 N:
    //     hash & (N - 1) == hash % N
    //   But AND is a single CPU instruction (1 cycle), while DIV is
    //   20-40 cycles on x86. Even if the compiler optimizes % to AND
    //   for compile-time constants, the AND makes the intent explicit
    //   and is guaranteed to be optimal.
    //
    // Distribution quality:
    //   AND with (N-1) takes the bottom log2(N) bits of the hash.
    //   For std::hash<string> (FNV-1a or similar), the low bits have
    //   good entropy. If the hash were weak (e.g., returning sequential
    //   numbers), the bottom bits would create hot shards. But standard
    //   library hashes are designed for uniform distribution.
    [[nodiscard]] std::size_t shard_index(std::string_view key) const noexcept {
        return std::hash<std::string_view>{}(key) & (NUM_SHARDS - 1);
    }
};
