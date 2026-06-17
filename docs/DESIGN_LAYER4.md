# Layer 4 — Write-Ahead Log (WAL): Design Decisions

## Overview

Layer 4 adds crash recovery via a Write-Ahead Log (WAL). Every mutating operation (SET, DEL) is written to an append-only binary file **before** the in-memory store is updated. On crash, the store is rebuilt by replaying the WAL from the beginning. This guarantees durability: if the client receives `+OK`, the data is on disk.

---

## Decision 1: WAL-First Protocol (Write-Ahead Logging)

### The Invariant
```
WAL write → Store update → Client ACK
```

If the server crashes at any point:
- **Before WAL write**: Data is lost, but the client never received `+OK` — so the client knows the write didn't succeed.
- **After WAL write, before store update**: On restart, the WAL replay re-applies the write. Data is recovered.
- **After store update, before client ACK**: Same as above. The client may retry, but the WAL replay is idempotent (SET overwrites, DEL is a no-op for missing keys).

This is the same protocol used by PostgreSQL, SQLite, LevelDB, and essentially every database that guarantees durability.

### Code Path
```cpp
// protocol.cpp — SET handler
if (wal != nullptr) {
    if (!wal->append_set(key, value)) {
        resp_error(out, "ERR WAL write failed");
        return;  // Store NOT modified
    }
}
store.set(key, value);
resp_simple_string(out, "OK");
```

If the WAL write fails (disk full, I/O error), the store is **not** modified and the client receives an error. This prevents the in-memory state from diverging from the durable state.

---

## Decision 2: Binary WAL Format

### Layout
```
┌──────────┬───────────┬────────┬─────────┬──────┬─────────┬───────┬───────┐
│entry_len │ timestamp │op_type │ key_len │ key  │ val_len │ value │ crc32 │
│  4 bytes │  8 bytes  │1 byte  │ 2 bytes │ var  │ 2 bytes │  var  │4 bytes│
└──────────┴───────────┴────────┴─────────┴──────┴─────────┴───────┴───────┘
```

### Why Binary (Not Text/JSON)?
| Format | SET "key" "value" size | Parse cost |
|--------|----------------------|------------|
| JSON `{"op":"SET","key":"key","value":"value"}` | ~50 bytes | JSON parser needed |
| Text `SET key value\n` | ~16 bytes | Delimiter scanning |
| Binary (ours) | 31 bytes | `memcpy` — zero parsing |

Binary is:
1. **Compact**: Fixed overhead of 17 bytes per entry (vs 30-50 for text)
2. **Fast to parse**: `memcpy` field extraction — no string scanning
3. **Unambiguous**: Length-prefixed fields handle keys/values with spaces or newlines
4. **CRC-protectable**: The exact byte sequence is what we checksum

### Why Little-Endian?
We target x86_64 only (Linux). Native byte order avoids endian conversion on every read/write. A `static_assert` in `wal.h` prevents compilation on big-endian platforms:

```cpp
static_assert(std::endian::native == std::endian::little,
    "WAL binary format assumes little-endian byte order (x86_64)");
```

This uses C++20's `<bit>` header — the first time `std::endian` appears in the project.

---

## Decision 3: CRC32 for Corruption Detection

### Why CRC32?
CRC32 detects all single-bit errors, all burst errors up to 32 bits, and 99.99999953% of all other errors. For a WAL (where corruption = partial write from a crash), this is more than sufficient.

### Why Not SHA-256 or xxHash?
| Algorithm | Throughput (x86_64) | Output size | Use case |
|-----------|-------------------|-------------|----------|
| CRC32 | ~10 GB/s | 4 bytes | Error detection |
| xxHash64 | ~15 GB/s | 8 bytes | Hash tables |
| SHA-256 | ~0.5 GB/s | 32 bytes | Cryptographic integrity |

CRC32 is the standard for storage systems (zlib, gzip, Ethernet frames, ext4 metadata). We don't need cryptographic guarantees — we're detecting accidental corruption, not adversarial tampering.

### Compile-Time Table Generation
```cpp
constexpr std::array<uint32_t, 256> make_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1u) ? ((crc >> 1) ^ 0xEDB88320u) : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}
constexpr auto kCrc32Table = make_crc32_table();
```

The lookup table (1KB) is computed at compile time using C++20 `constexpr`. Zero runtime initialization cost — the table is embedded directly in the binary.

### Verification
CRC32 of `"123456789"` = `0xCBF43926` (standard test vector, verified in unit tests).

---

## Decision 4: pwrite() for Atomic Positional Writes

### Why pwrite Instead of write?
```
write():   lseek(fd, pos) → write(fd, buf, len)    // 2 syscalls, race window
pwrite():  pwrite(fd, buf, len, pos)                // 1 syscall, atomic position
```

`pwrite()` is atomic: it writes at a specific file offset without modifying the file's current position. Two threads calling `pwrite()` concurrently write to their correct offsets without interleaving. With `write()`, a thread could be preempted between `lseek()` and `write()`, causing another thread's data to land at the wrong offset.

Although our server is currently single-threaded, using `pwrite()` prepares for Layer 5 (replication) and demonstrates the atomic write pattern expected in systems engineering interviews.

### Write Serialization
Even with `pwrite()`, we serialize appends with a `std::mutex` because:
1. Two threads could read the same `write_pos_`, leading to overlapping writes
2. The `write_pos_ += total_size` update is not atomic with the `pwrite()` call

---

## Decision 5: fdatasync() vs fsync()

### The Difference
| Function | Flushes data? | Flushes metadata? | Cost |
|----------|--------------|-------------------|------|
| `fsync()` | Yes | Yes (all metadata) | ~500μs |
| `fdatasync()` | Yes | Only size changes | ~300μs |
| No sync | No | No | ~0μs |

`fdatasync()` is faster because it skips metadata that's irrelevant for data recovery (timestamps, permissions). It still flushes the file size, which is the only metadata needed to locate the appended data.

This is the standard choice for database WALs. PostgreSQL, LevelDB, and RocksDB all use `fdatasync()`.

### Sync Policies
```
--sync every     fdatasync() after every append (default, safest)
--sync periodic  fdatasync() every 100ms via background thread
--sync none      OS decides when to flush (fastest, for testing)
```

The periodic policy uses a background thread with `condition_variable::wait_for()`:
```cpp
void sync_thread_func() {
    while (sync_running_.load()) {
        std::unique_lock lock(sync_cv_mutex_);
        sync_cv_.wait_for(lock, std::chrono::milliseconds(sync_interval_ms_));
        if (sync_running_.load() && fd_ != -1) {
            fdatasync(fd_);
        }
    }
}
```

The `wait_for()` pattern allows the thread to wake up early for clean shutdown (via `notify_one()`).

---

## Decision 6: Recovery Semantics — Stop at First Corruption

### The Problem
After a crash, the WAL file might contain:
1. **Complete, valid entries** (CRC matches)
2. **A partially written entry** (crash during `pwrite`)
3. **Garbage bytes** (if the OS wrote partial data)

### Our Strategy
```
Entry1 ✓ → replay → Entry2 ✓ → replay → Entry3 ✗ CRC mismatch → STOP
```

We replay entries sequentially. At the first CRC mismatch or incomplete entry, we **stop** and **truncate** the file at the last valid entry. This:

1. **Prevents replaying corrupted data** (CRC mismatch = unknown state)
2. **Cleans up garbage** (future appends start after the last valid entry)
3. **Is simple and predictable** (no "skip and continue" ambiguity)

### Alternative: Checkpointing (Not Implemented)
Production databases periodically take snapshots (checkpoints) and truncate the WAL. Without checkpoints, our WAL grows unboundedly. This is acceptable for a portfolio project but would be addressed in production with:
- Periodic snapshots (dump the entire store to disk)
- WAL compaction (merge old entries, drop overwritten keys)
- WAL rotation (rename and start a new file)

---

## Decision 7: Stack-Allocated Serialization Buffer

```cpp
uint8_t stack_buf[4096];
std::vector<uint8_t> heap_buf;
uint8_t* buf;

if (total_size <= sizeof(stack_buf)) {
    buf = stack_buf;  // Hot path: zero allocation
} else {
    heap_buf.resize(total_size);
    buf = heap_buf.data();  // Cold path: heap for large values
}
```

Most KV entries are small (key: 10-50 bytes, value: 10-500 bytes). The fixed overhead is 21 bytes (4+8+1+2+2+4), so a 4KB stack buffer handles values up to ~4050 bytes — covering 99.9% of real-world entries.

The heap fallback (`std::vector`) handles arbitrarily large values without a hard limit.

---

## Decision 8: system_clock for Timestamps (Not steady_clock)

### In the Store (Layer 3)
We used `steady_clock` for TTL because:
- TTL measures **elapsed time** (monotonic)
- `system_clock` can jump backward (NTP), un-expiring keys

### In the WAL (Layer 4)
We use `system_clock` for timestamps because:
- WAL entries need **wall-clock time** for debugging ("when did this write happen?")
- `steady_clock` values are meaningless across restarts (epoch is undefined)
- WAL timestamps are informational only — they don't affect correctness

---

## Test Coverage

| Category | Tests | What's Verified |
|----------|-------|----------------|
| CRC32 | 3 | Empty input, standard test vector, determinism |
| Write + replay | 3 | SET entries, DEL entries, empty WAL |
| Corruption | 2 | CRC mismatch in first entry, second entry |
| Partial write | 2 | Truncated entry, partial entry_len field |
| Truncation | 1 | WAL truncate + rewrite |
| Large entry | 1 | Value exceeding 4KB stack buffer |
| Append after replay | 1 | Write → close → replay → write → replay |
| Stress | 1 | 10,000 entries write + replay |
| **Total WAL** | **13** | |
| **Grand total** | **62** | 31 protocol + 18 store + 13 WAL |

### Live Crash Recovery Test
```
Phase 1: SET user:1 alice, SET user:2 bob, SET user:3 charlie, DEL user:2
         → Kill server (simulated crash)
Phase 2: Restart → WAL replays 4 entries
         → GET user:1 → alice   ✓
         → GET user:2 → nil     ✓ (DEL replayed)
         → GET user:3 → charlie ✓
```

---

## What Layer 5 Changes

In Layer 5 (Single-Leader Replication):

1. **New files**: `replication.h`, `replication.cpp`
2. **Leader**: Accepts writes, forwards WAL entries to replicas
3. **Replica**: Receives WAL entries from leader, applies to local store
4. **CLI flags**: `--role leader` or `--role replica --leader-host <host> --leader-port <port>`
5. **Consistency**: Eventual consistency — replicas lag behind leader by network latency
