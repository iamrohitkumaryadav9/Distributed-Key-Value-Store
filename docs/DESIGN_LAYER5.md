# Layer 5 — Single-Leader Replication: Design Decisions

## Overview

Layer 5 adds single-leader replication. One server acts as the **leader** (accepts reads and writes), and one or more servers act as **replicas** (serve read-only queries). When the leader processes a write (SET, DEL), it broadcasts the WAL entry to all connected replicas, which apply it to their local stores.

This provides **horizontal read scaling** (distribute GET load across replicas) and **data redundancy** (data survives leader failure if at least one replica has it).

---

## Decision 1: Asynchronous Replication (Not Synchronous)

### What This Means
```
Client → Leader: SET key value
Leader: WAL write → store update → broadcast to replicas → +OK to client
                                    ↑ Does NOT wait for replica ACK
```

The leader acknowledges the write to the client **before** waiting for replicas to confirm. This means:

- **Fastest possible write latency**: The client doesn't pay for network RTT to replicas.
- **Trade-off**: If the leader crashes immediately after ACK but before the replica receives the entry, that write is lost.

### Why Not Synchronous?
| Property | Async | Sync (majority ACK) | Sync (all ACK) |
|----------|-------|---------------------|----------------|
| Write latency | ~WAL latency | ~2× network RTT | ~slowest replica RTT |
| Availability | Leader crash = lost recent writes | Survives minority failures | Any replica down = writes blocked |
| Complexity | Low | Medium (quorum logic) | Low but fragile |

Synchronous replication requires waiting for replica ACKs, which blocks the single-threaded event loop. For a portfolio project demonstrating the concept, async replication is the right choice. Production systems (Redis, PostgreSQL streaming replication) default to async as well.

---

## Decision 2: Reusing WAL Binary Format for Replication

### The Key Insight
The WAL binary format (entry_len + timestamp + op + key + value + crc32) is already defined, serialized, parsed, and CRC-validated. Instead of inventing a separate replication protocol, we send raw WAL entries over TCP.

```
Leader WAL file:          [entry1][entry2][entry3]...
Leader → Replica TCP:     [entry1][entry2][entry3]...  (same bytes)
```

### Benefits
1. **Zero new serialization code**: The same `append_entry()` serialization produces both the WAL file bytes and the replication stream bytes.
2. **CRC32 validation for free**: The replica verifies every received entry with the same CRC32 logic used for WAL replay.
3. **Consistent format**: A hex dump of the replication stream is identical to a hex dump of the WAL file — easy to debug.

### How Bytes Are Captured
```cpp
// WAL::append_set now has an optional out_bytes parameter:
bool append_set(std::string_view key, std::string_view value,
                std::vector<uint8_t>* out_bytes = nullptr);

// In dispatch_command (SET handler):
std::vector<uint8_t> wal_bytes;
wal->append_set(key, value, repl ? &wal_bytes : nullptr);
if (repl) repl->broadcast(wal_bytes.data(), wal_bytes.size());
```

When `repl` is null (standalone mode), no vector allocation occurs — the code path is identical to Layer 4.

---

## Decision 3: Full Sync for New Replicas

### The Problem
When a new replica connects, it has an empty store. It needs the leader's complete state before it can serve reads.

### Our Approach: Send the Entire WAL File
```
Replica connects → Leader reads WAL file → sends all bytes → adds replica to list
```

This is the simplest correct approach:
- The replica parses the byte stream as a sequence of WAL entries (using the same code as `WAL::replay()`)
- No distinction between "initial sync" and "live updates" — it's a continuous stream of WAL entries

### The Race Condition Fix
A subtle race exists: what if a new write arrives while we're sending the full sync?

```
Timeline:
  T1: Replica connects, leader starts full sync
  T2: Client sends SET key value → WAL write + broadcast()
  T3: Leader finishes full sync, adds replica to list
```

At T2, the replica is NOT in the list yet, so `broadcast()` doesn't reach it. But the new WAL entry might not be in the portion of the file we already sent.

**Solution**: We hold `replicas_mutex_` during both the full sync AND the list insertion:

```cpp
{
    std::lock_guard lock(replicas_mutex_);
    send_full_sync(replica_fd);      // Reads entire WAL file
    replica_fds_.push_back(replica_fd);  // Now in the list
}
```

Since `broadcast()` also acquires `replicas_mutex_`, it blocks during full sync. Any write that happens during this window:
1. Gets appended to the WAL file (before broadcast tries to acquire the lock)
2. Gets included in the full sync (because the file now has the new entry)
3. Also gets broadcast after the lock is released (but the replica already has it)

Since SET is idempotent, applying an entry twice is harmless.

---

## Decision 4: Separate Replication Port

### Architecture
```
Leader (port 6379):  Client RESP connections (SET, GET, DEL, PING)
Leader (port 6380):  Replica connections (WAL binary stream)

Replica (port 6381): Client RESP connections (GET, PING only)
Replica (internal):  Connection to leader:6380
```

### Why Not Share One Port?
1. **Protocol separation**: The client port speaks RESP (text protocol), the replication port speaks WAL binary. Mixing them requires a protocol negotiation handshake.
2. **Security**: In production, you'd firewall the replication port to only accept connections from known replicas.
3. **Simplicity**: Two simple listeners are easier to reason about than one multiplexed listener.

Redis uses the same approach: port 6379 for clients, and a separate internal connection for replication (though Redis reuses the same port with a REPLCONF handshake).

---

## Decision 5: Read-Only Replicas

### Replica Client Behavior
```
Client → Replica: GET key       → $<len>\r\n<value>\r\n   (served locally)
Client → Replica: SET key value → -READONLY You can't write against a read-only replica\r\n
Client → Replica: DEL key       → -READONLY You can't write against a read-only replica\r\n
Client → Replica: PING          → +PONG\r\n                  (always works)
```

### Implementation
```cpp
void dispatch_command(..., bool read_only) {
    // SET handler
    if (read_only) {
        resp_error(out, "READONLY You can't write against a read-only replica");
        return;
    }
    // ... proceed with WAL write + store update
}
```

The `read_only` flag is set in main.cpp based on `--role replica`. This is checked at the dispatch level, before any WAL or store operation. The error message follows the Redis convention.

---

## Decision 6: No Local WAL on Replicas

### Trade-off
| Approach | Crash recovery | Disk I/O | Complexity |
|----------|---------------|----------|------------|
| Replica WAL | Fast (replay local WAL) | 2× writes (local WAL + receive) | Higher |
| No replica WAL | Slow (full sync from leader) | 1× writes (receive only) | Lower |

We chose **no replica WAL** because:
1. Full sync from leader is simple and correct
2. Replicas are stateless — you can add/remove them freely
3. The full sync is fast for reasonable WAL sizes (streaming sequential I/O)
4. Production systems do support both, but local replica AOF is a Layer 6+ concern

If the replica crashes, it simply reconnects to the leader and gets a fresh full sync.

---

## Decision 7: poll() for Clean Accept Loop Shutdown

### The Problem
The accept loop runs in a background thread. When we call `stop()`, we need the thread to exit cleanly. But `accept()` blocks indefinitely.

### Options
| Approach | Mechanism | Clean? |
|----------|-----------|--------|
| `close(listen_fd)` during accept | `accept()` returns EBADF | Racy |
| Non-blocking fd + busy loop | `fcntl(O_NONBLOCK)` + sleep | Wastes CPU |
| `poll()` with timeout | `poll(fd, POLLIN, 500ms)` | Clean ✓ |
| `eventfd` or pipe for signaling | Write to wakeup fd | Cleanest, more code |

We use `poll()` with a 500ms timeout:
```cpp
while (running_.load()) {
    struct pollfd pfd{listen_fd_, POLLIN, 0};
    int ret = poll(&pfd, 1, 500);  // 500ms timeout
    if (ret <= 0) continue;
    if (!running_.load()) break;
    int client_fd = accept4(listen_fd_, ...);
    // ...
}
```

This checks `running_` every 500ms, providing clean shutdown without busy-waiting. The latency for accepting a new replica is at most 500ms, which is acceptable for replication setup.

---

## Decision 8: shutdown() Before close() for Replica Socket

### The Problem
The replica's receive loop does blocking `read()`. Calling `close(fd)` from another thread while `read()` is blocked causes undefined behavior (POSIX says closing a fd while another thread is blocked on it is UB).

### The Fix
```cpp
void ReplicaClient::stop() {
    running_.store(false);
    shutdown(fd_, SHUT_RDWR);  // Unblocks read()
    close(fd_);
    recv_thread_.join();
}
```

`shutdown(SHUT_RDWR)` causes the blocked `read()` to return 0 (EOF), which cleanly exits the receive loop. Then we `close()` and `join()` the thread.

---

## Test Coverage

| Category | Tests | What's Verified |
|----------|-------|----------------|
| Lifecycle | 1 | ReplicationManager start/stop without crash |
| Full sync | 1 | Existing WAL data reaches new replica (4 entries) |
| Live replication | 1 | Writes after replica connects arrive correctly |
| Read-only guard | 1 | SET/DEL rejected, GET/PING allowed on replica |
| Multiple replicas | 1 | All 3 replicas receive same broadcast |
| Reconnect | 1 | Disconnected replica gets full state on reconnect |
| **Total Layer 5** | **6** | |
| **Grand total** | **68** | 31 protocol + 18 store + 13 WAL + 6 replication |

### Live E2E Test
```
Leader (port 6379):   SET user:1 alice, SET user:2 bob, SET user:3 charlie, DEL user:2
Replica (port 6381):  GET user:1 → alice ✓, GET user:2 → nil ✓, GET user:3 → charlie ✓
                      SET → READONLY ✓, DEL → READONLY ✓
                      PING → PONG ✓ (both)
```

---

## What Would Come Next (Layer 6+)

In a production system, the following would be added:

1. **Automatic failover**: If the leader crashes, promote a replica automatically (Raft or leader election).
2. **Replication offset tracking**: Instead of full sync, replicas track their offset and request only missing entries on reconnect (partial resynchronization).
3. **WAL compaction/snapshots**: Periodically take a store snapshot and truncate the WAL to prevent unbounded growth.
4. **Replica-local WAL**: Write received entries to a local WAL for fast crash recovery without full sync.
5. **Read-your-writes consistency**: Route reads to the leader after a write to avoid stale reads.
6. **Configurable consistency levels**: Allow clients to choose between "read from any replica" (fastest) and "read from leader" (strongest).
