# Layer 3 — Sharded Concurrent Hash Map: Design Decisions

## Overview

Layer 3 replaces the single-threaded `SimpleStore` with a production-ready sharded concurrent hash map (`Store<NumShards>`). The store partitions data across 16 independent shards, each with its own `std::shared_mutex`, enabling concurrent access from multiple threads. TTL (time-to-live) support is added with lazy expiration using `std::chrono::steady_clock`.

---

## Decision 1: Why Shard the Hash Map?

### The Problem
A single `std::unordered_map` + `std::mutex` creates a serialization bottleneck: every GET blocks every SET, and vice versa. Under 4 concurrent threads, this single-lock design limits throughput to approximately what a single thread can achieve.

### The Solution: Sharding
Split the hash map into N independent shards, each with its own lock:

```
Key → hash(key) → shard_index → Shard[i] → {map, mutex}
```

With 16 shards, threads operating on keys in different shards never contend. The probability of two random keys landing in the same shard is 1/16 = 6.25%.

### Benchmark Results (4 threads, 1M operations)

| Configuration | SET throughput | GET throughput | SET p99 |
|--------------|----------------|----------------|---------|
| Store<1> (1 shard) | 1.95 M ops/sec | 11.42 M ops/sec | 12,285ns |
| Store<16> (16 shards) | 4.69 M ops/sec | 14.40 M ops/sec | 2,856ns |
| **Speedup** | **2.40x** | **1.26x** | **4.3x better** |

**Key insight**: SETs benefit more from sharding (2.4x) because they take exclusive locks. GETs benefit less (1.26x) because `shared_mutex` already allows concurrent reads within a single shard.

### Why 16 Shards?
- **Power of 2**: Required for bitwise AND sharding (see Decision 2).
- **Matches typical core counts**: Modern CPUs have 4-16 cores. More shards than cores wastes memory without reducing contention.
- **Diminishing returns**: Going from 1 → 16 shards gives ~2.4x throughput. Going from 16 → 256 would give minimal additional benefit because contention is already low.

---

## Decision 2: Bitwise AND Sharding vs. Modulo

### Shard Selection Formula
```cpp
shard_index = hash(key) & (NUM_SHARDS - 1)
```

### Why AND Instead of Modulo?

| Operation | CPU Cycles (x86_64) | Predictable? |
|-----------|-------------------|-------------|
| `hash & 15` (AND) | 1 cycle | Yes — branchless |
| `hash % 16` (DIV) | 20-40 cycles | No — depends on divisor |

For a power-of-2 N, `hash & (N-1)` is mathematically equivalent to `hash % N`. The compiler might optimize `% 16` to AND, but only if it can prove at compile time that N is a power of 2. Our `static_assert` guarantees this, but the explicit AND makes the optimization visible and intentional.

### Distribution Quality
AND with (N-1) extracts the bottom `log2(N)` bits of the hash. This works well because `std::hash<string>` (typically FNV-1a or xxHash on libstdc++) distributes entropy uniformly across all bits. We verified distribution in tests: 1000 random keys hit all 16 shards.

### The static_assert Guard
```cpp
static_assert((NUM_SHARDS & (NUM_SHARDS - 1)) == 0,
    "NUM_SHARDS must be a power of 2");
```

This uses the classic power-of-2 check: a number N is a power of 2 iff `N & (N-1) == 0` (for N > 0). If someone changes NUM_SHARDS to 12, the build fails immediately with a clear message.

---

## Decision 3: `alignas(64)` — Preventing False Sharing

### What is False Sharing?
Modern CPUs don't read individual bytes from memory — they read **64-byte cache lines**. If two cores write to different variables that happen to live on the same cache line, the CPU's cache coherence protocol (MESI/MOESI) forces both cores to invalidate and reload the entire cache line. This is "false sharing" — contention caused by proximity in memory, not by actual data dependencies.

### The Problem Without alignas
```
Memory layout without alignment:
[Shard0.map...Shard0.mutex|Shard1.map...] ← Same cache line!
         Thread A writes ↑     ↑ Thread B reads
```
Thread A locking Shard 0's mutex invalidates Thread B's cache line for Shard 1's map. This can degrade performance by **10-50x**.

### The Fix
```cpp
struct alignas(64) Shard {
    std::unordered_map<...> data;
    mutable std::shared_mutex mutex;
};
```

`alignas(64)` forces each Shard to start at a 64-byte boundary. Since each Shard is larger than 64 bytes (map + mutex), it spans multiple cache lines, but the **boundary** between shards is always on a cache line boundary.

### Cost
With alignas(64), each Shard is rounded up to the next 64-byte multiple. With 16 shards, the total padding overhead is at most ~450 bytes — negligible.

### Interview Note
False sharing is one of the most common concurrency performance bugs. In HFT, it's checked for in every shared data structure. Tools like `perf c2c` can detect false sharing at runtime.

---

## Decision 4: `std::shared_mutex` — Reader-Writer Locking

### Why Not a Regular Mutex?
KV store workloads are heavily read-biased (typically 90%+ GETs). A regular `std::mutex` serializes all access — 10 concurrent GETs would execute sequentially. `std::shared_mutex` allows:

- **Multiple concurrent readers**: `std::shared_lock` (non-exclusive)
- **Single exclusive writer**: `std::unique_lock` (exclusive)

### Locking Strategy

| Operation | Lock Type | Blocks Readers? | Blocks Writers? |
|-----------|-----------|-----------------|-----------------|
| GET | `shared_lock` | No | Yes (waits) |
| SET | `unique_lock` | Yes (waits) | Yes (waits) |
| DEL | `unique_lock` | Yes (waits) | Yes (waits) |

### Benchmark Impact
The GET speedup (1.26x) is smaller than SET (2.40x) precisely because `shared_lock` already allows concurrent reads within a single shard. Sharding primarily helps writes.

### Alternatives Considered

| Approach | Pros | Cons |
|----------|------|------|
| `std::mutex` | Simple, no reader/writer distinction | Serializes reads unnecessarily |
| `std::shared_mutex` | Concurrent reads ✓ | Writer starvation possible |
| Lock-free hash map | Maximum throughput | Extremely complex, ABA problems |
| RCU (Read-Copy-Update) | Near-zero read overhead | Complex, delayed memory reclamation |

We chose `shared_mutex` as the best balance of correctness, simplicity, and performance.

---

## Decision 5: `std::optional<std::string>` Return from GET

### The Problem
In Layer 2, `SimpleStore::get()` returned `const std::string*` — a pointer into the map's internal storage. This was safe because:
1. The server is single-threaded
2. The pointer is used immediately (before any mutation)

With concurrent access, the pointer could dangle: another thread might delete or rehash the entry after we release the lock.

### The Solution: Return by Value
```cpp
std::optional<std::string> get(std::string_view key) {
    std::shared_lock lock(shard.mutex);
    auto it = shard.data.find(key);
    if (it == shard.data.end()) return std::nullopt;
    return it->second.value;  // Copy while holding the lock
}
```

The `std::string` copy happens inside the locked scope, guaranteeing consistency. After the lock is released, the caller owns the string.

### Performance Cost
The copy costs:
- **SSO values (≤15 chars on libstdc++)**: Zero heap allocation — copied into the small buffer
- **Larger values**: One `malloc` + `memcpy`

For typical KV workloads (keys: 10-50 bytes, values: 10-500 bytes), this is acceptable. For maximum performance, alternatives include:
1. **Callback pattern**: `store.get(key, [](string_view val) { ... })` — processes the value while the lock is held
2. **Lock guard wrapper**: Returns a RAII handle that holds the shared_lock until the caller is done

---

## Decision 6: TTL with Lazy Expiration

### Design
Each entry stores an optional expiry timestamp:
```cpp
struct Entry {
    std::string value;
    std::optional<steady_clock::time_point> expiry;
};
```

### Why `steady_clock` (Not `system_clock`)?

| Clock | Monotonic? | Adjustable by NTP? |
|-------|-----------|-------------------|
| `system_clock` | No | Yes — can jump forward/backward |
| `steady_clock` | Yes | No — only moves forward |

If the system clock jumps backward (NTP correction, daylight saving), keys would appear to "un-expire." `steady_clock` is immune to this because it's guaranteed monotonic.

### Lazy Expiration Strategy
Expired keys are deleted when accessed (on GET), not by a background thread:

```
GET "expired_key":
  1. shared_lock → find entry → check is_expired()
  2. If expired:
     a. Release shared_lock
     b. Acquire unique_lock
     c. Re-check (TOCTOU protection)
     d. Delete if still expired
  3. Return nullopt
```

### Why Lazy Instead of Active Expiration?
- **Active** (Redis approach): A background thread periodically scans for expired keys. Guarantees timely cleanup but adds complexity (thread synchronization, CPU cost).
- **Lazy** (our approach): Only check on access. Simpler, zero CPU cost for keys that are never accessed again. Downside: expired keys consume memory until accessed.

For a portfolio project, lazy expiration is sufficient and demonstrates the core concept. Redis actually uses both: lazy on access + active periodic scan.

### TOCTOU Protection
Between releasing the shared_lock and acquiring the unique_lock, another thread might:
1. Delete the key (fine — our re-check handles this)
2. Update the key with a new value (we check `is_expired()` again — if the new entry isn't expired, we don't delete it)
3. Do nothing (most common — we delete as expected)

---

## Decision 7: Template Parameter for Shard Count

### Design
```cpp
template<std::size_t NumShards = 16>
class Store { ... };

using DefaultStore = Store<>;  // Store<16>
```

### Why a Template (Not Runtime Parameter)?
1. **Compile-time optimization**: The compiler unrolls `shard_index = hash & (N-1)` with a constant mask. With a runtime N, it would be a load + AND.
2. **static_assert enforcement**: We can enforce power-of-2 at compile time. A runtime check would only catch violations after deployment.
3. **Separate instantiations for benchmarking**: `Store<1>` and `Store<16>` are separate types with separate code paths, making the benchmark apples-to-apples.

### Explicit Template Instantiation
```cpp
// store.cpp
template class Store<1>;
template class Store<16>;
```

Method bodies live in store.cpp, not in the header. This:
1. **Reduces compile time**: Headers are parsed by every .cpp that includes them. Method bodies in the header would be compiled N times.
2. **Controls available instantiations**: Trying to use `Store<7>` would cause a linker error, catching the power-of-2 violation even without the static_assert.

---

## Decision 8: Transparent Hash — Zero-Allocation Lookups

TransparentStringHash and TransparentStringEqual were moved from protocol.h to store.h because they're primarily a store concern. The `is_transparent` tag (C++20, P0919R3) enables `find(string_view)` on an `unordered_map<string, ...>` without creating a temporary `std::string`.

**Critical invariant**: `hash(string_view("foo")) == hash(string("foo"))`. Our implementation guarantees this because both hash through `std::hash<string_view>`, which hashes the byte sequence — identical for both types.

---

## Test Coverage

| Category | Tests | What's Verified |
|----------|-------|----------------|
| Protocol parser | 11 | Zero-copy, partial reads, edge cases |
| Response formatters | 7 | All RESP types, batch append |
| Command dispatch | 8 | All commands via Store<16> |
| Integration | 3 | Multi-command buffers, partial commands |
| **Store — basic** | **7** | **get, set, del, overwrite, multiple keys** |
| **Store — TTL** | **4** | **not expired, expired, overwrite removes TTL, lazy deletion** |
| **Store — sharding** | **2** | **distribution across 16 shards, Store<1> correctness** |
| **Store — threads** | **3** | **concurrent reads, writes, mixed read-write** |
| **Store — compile** | **1** | **static_assert constraints** |
| **Total** | **49** (prev 37) | **+18 new store tests, -6 SimpleStore tests** |

---

## What Layer 4 Changes

In Layer 4 (WAL + Crash Recovery):

1. **New files**: `wal.h`, `wal.cpp` — Write-Ahead Log
2. **store integration**: Every SET/DEL writes to the WAL before mutating the store
3. **Recovery**: On startup, replay the WAL to rebuild the store state
4. **Fsync policy**: Configurable durability (every write vs. periodic)
