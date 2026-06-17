// ─────────────────────────────────────────────────────────────────────────────
// store.cpp — Sharded concurrent hash map implementation
//
// This file contains the method definitions for the Store template class
// and explicit template instantiations for the shard counts we use:
//   - Store<16>: The production configuration (used by the server)
//   - Store<1>:  Single-shard baseline (used by the benchmark for comparison)
//
// Explicit instantiation pattern:
//   Template method bodies live here (not in the header) to:
//   1. Reduce compile time: each .cpp that includes store.h doesn't
//      re-compile the method bodies.
//   2. Control what's instantiated: only Store<1> and Store<16> are
//      available. Attempting to use Store<7> would be a linker error.
//   3. Keep the header clean: declarations only, no implementation clutter.
// ─────────────────────────────────────────────────────────────────────────────

#include "store.h"

#include <mutex>    // std::shared_lock, std::unique_lock

// ─────────────────────────────────────────────────────────────────────────────
// get — Read a key (shared lock, lazy expiration)
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
std::optional<std::string> Store<NumShards>::get(std::string_view key) {
    auto& shard = shards_[shard_index(key)];

    // ── Optimistic read path: shared lock ───────────────────────────────
    // Multiple threads can hold shared_lock simultaneously — readers don't
    // block each other. This is the hot path: most KV workloads are 90%+
    // reads.
    {
        std::shared_lock lock(shard.mutex);
        auto it = shard.data.find(key);

        if (it == shard.data.end()) {
            return std::nullopt;  // Key does not exist
        }

        if (!it->second.is_expired()) {
            // Happy path: key exists and hasn't expired.
            // Return a COPY of the value. We must copy while holding the lock
            // because after lock release, the entry could be modified or
            // deleted by another thread.
            return it->second.value;
        }
    }
    // shared_lock released here (RAII)

    // ── Expired key cleanup: unique lock ────────────────────────────────
    // The key exists but has expired. We need to delete it, which requires
    // exclusive (unique) access. std::shared_mutex does NOT support
    // upgrading a shared_lock to a unique_lock — we must release the
    // shared_lock first, then acquire a unique_lock.
    //
    // This creates a TOCTOU (time-of-check-time-of-use) window: between
    // releasing the shared lock and acquiring the unique lock, another
    // thread might have already deleted the key or updated it with a new
    // value. We must re-check the entry's state under the unique lock.
    {
        std::unique_lock lock(shard.mutex);

        // Re-check: the entry might have been deleted or replaced since
        // we released the shared lock.
        auto it = shard.data.find(key);
        if (it != shard.data.end() && it->second.is_expired()) {
            shard.data.erase(it);
        }
    }

    return std::nullopt;
}

// ─────────────────────────────────────────────────────────────────────────────
// set — Insert or update a key-value pair (no TTL)
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
void Store<NumShards>::set(std::string_view key, std::string_view value) {
    auto& shard = shards_[shard_index(key)];

    // Unique lock: exclusive access. No other thread can read or write
    // this shard while we hold the lock.
    std::unique_lock lock(shard.mutex);

    // insert_or_assign: atomically inserts a new entry or overwrites
    // an existing one. The key and value are copied into owned std::string
    // storage — necessary because string_views reference the client's
    // read buffer, which will be reused for the next command.
    //
    // The expiry is set to nullopt — this key lives forever (or until DEL).
    shard.data.insert_or_assign(
        std::string(key),
        Entry{std::string(value), std::nullopt}
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// set_with_ttl — Insert or update with a time-to-live
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
void Store<NumShards>::set_with_ttl(std::string_view key, std::string_view value,
                                    std::chrono::milliseconds ttl) {
    auto& shard = shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);

    // Calculate the absolute expiry time from "now + ttl."
    // Using steady_clock::now() inside the lock means the TTL countdown
    // starts from the moment we write, not from when the client sent the
    // command. The difference is negligible (microseconds) but worth noting.
    auto expiry_time = std::chrono::steady_clock::now() + ttl;

    shard.data.insert_or_assign(
        std::string(key),
        Entry{std::string(value), expiry_time}
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// del — Delete a key
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
bool Store<NumShards>::del(std::string_view key) {
    auto& shard = shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);

    // find + erase-by-iterator pattern.
    // We use find() with heterogeneous lookup (string_view → no temp string)
    // and then erase the iterator. Using erase(key) directly would require
    // constructing a temporary std::string from the string_view.
    auto it = shard.data.find(key);
    if (it == shard.data.end()) {
        return false;
    }
    shard.data.erase(it);
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Explicit template instantiations
// ─────────────────────────────────────────────────────────────────────────────
// Only these two specializations are compiled. Any other shard count used
// in a .cpp file would cause a linker error unless added here.
//
// Store<16>: Production configuration used by the server.
// Store<1>:  Single-shard baseline used by the benchmark to quantify
//            the benefit of sharding under contention.

template class Store<1>;
template class Store<16>;
