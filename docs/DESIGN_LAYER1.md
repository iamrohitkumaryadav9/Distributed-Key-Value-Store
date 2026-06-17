# Layer 1 — Async TCP Server: Design Decisions

## Overview

Layer 1 implements a single-threaded, non-blocking TCP server using Linux's `epoll` system call in edge-triggered mode. The server accepts connections, reads data, and echoes it back — validating the entire I/O pipeline before we add protocol parsing in Layer 2.

---

## Decision 1: Why `epoll` over `select`/`poll`/`io_uring`?

### The Problem
We need to multiplex I/O across hundreds or thousands of simultaneous TCP connections on a single thread.

### Alternatives Considered

| Approach | Time Complexity | FD Limit | Kernel Support |
|----------|----------------|----------|----------------|
| `select()` | O(n) per call — kernel scans all FDs | 1024 (FD_SETSIZE) | All UNIX |
| `poll()` | O(n) per call — same scan, no FD limit | Unlimited | All UNIX |
| `epoll()` | O(1) for readiness notification | Unlimited | Linux 2.6+ |
| `io_uring` | O(1), true async (no syscall per I/O) | Unlimited | Linux 5.1+ |

### Why `epoll`

1. **O(1) readiness notification**: `epoll` maintains a kernel-side interest list (a red-black tree) and a ready list. When a socket becomes readable, the kernel adds it to the ready list. `epoll_wait()` returns only the ready fds — no scanning required. With 10,000 connections where 5 are active, `select` scans 10,000 fds; `epoll` returns 5.

2. **No `FD_SETSIZE` limit**: `select()` is limited to 1024 fds by default (a macro, not easily changed). `poll()` removes this limit but still has O(n) scanning.

3. **Production-proven**: Redis, Nginx, HAProxy, and every major Linux network server uses `epoll`. It's the industry standard for TCP servers on Linux.

4. **Why not `io_uring`?**: `io_uring` is the future of Linux I/O — it avoids syscalls entirely by using shared memory ring buffers between userspace and kernel. However:
   - Requires Linux 5.1+ (our target is Ubuntu 22.04 = kernel 5.15, so technically possible)
   - The API is significantly more complex (submission queue, completion queue, SQE/CQE management)
   - Fewer engineers are familiar with it — `epoll` is more defensible in interviews
   - For a portfolio project, demonstrating `epoll` mastery is more valuable than `io_uring` novelty
   - We can always add an `io_uring` backend later behind an abstraction

---

## Decision 2: Edge-Triggered vs Level-Triggered

### The Problem
`epoll` supports two notification modes. The choice affects correctness and performance.

### How They Work

**Level-Triggered (LT)** — the default:
- `epoll_wait()` reports a fd as ready **as long as** the condition holds
- If you read 100 bytes from a socket that has 500 bytes, the next `epoll_wait()` will report it as readable again (because 400 bytes remain)
- Simpler to program: you can read partial data and the kernel will remind you

**Edge-Triggered (ET)** — `EPOLLET`:
- `epoll_wait()` reports a fd as ready **only when** the condition transitions (edge)
- If 500 bytes arrive and you read only 100, `epoll_wait()` will NOT report it again until NEW data arrives
- You MUST drain the fd completely (read until `EAGAIN`) or data will be silently stranded

### Why Edge-Triggered

1. **Fewer syscalls under load**: With LT, a socket with data generates an event on EVERY `epoll_wait()` call until drained. If we have 100 readable sockets and process 10 per loop iteration, the other 90 generate events every time — 90 redundant events per iteration. With ET, each socket generates exactly ONE event regardless of how many iterations pass.

2. **Predictable performance**: ET gives us control over the processing schedule. We drain each fd completely when we handle it, so we know the exact state of every connection at all times.

3. **Redis uses ET**: Following Redis's design validates our approach.

4. **Trade-off — correctness burden**: The programmer must:
   - Read/write in a loop until `EAGAIN`
   - Use `accept4` in a loop for multiple pending connections
   - Handle `EINTR` (signal interruption) correctly

   This is more code, but it's deterministic code with well-defined behavior.

---

## Decision 3: Non-Blocking Sockets (`O_NONBLOCK`)

### Why Non-Blocking?

In a single-threaded server, a blocking `read()` or `accept()` on one connection **stalls ALL connections**. Non-blocking mode makes `read()`/`write()` return immediately with `EAGAIN` when no data is available / the send buffer is full.

### Setting Non-Blocking

We use two mechanisms:
1. **`SOCK_NONBLOCK` flag**: Passed to `socket()` for the listener and `accept4()` for client sockets. This atomically creates the fd in non-blocking mode.
2. **`fcntl(F_SETFL, O_NONBLOCK)`**: Available as a fallback for sockets we don't create ourselves.

Why `SOCK_NONBLOCK` over `fcntl`?
- It's atomic — no race window between `socket()`/`accept()` and `fcntl()`
- In a multi-threaded setup, another thread could interact with the fd before `fcntl` runs

---

## Decision 4: Socket Options — `SO_REUSEADDR`, `SO_REUSEPORT`, `TCP_NODELAY`

### `SO_REUSEADDR`
**Problem**: After stopping the server, `bind()` fails with `EADDRINUSE` for ~60 seconds.

**Why**: TCP's TIME_WAIT state. When a connection closes, the side that initiates the close enters TIME_WAIT for 2×MSL (Maximum Segment Lifetime, typically 60s). This prevents late-arriving packets from being misinterpreted by a new connection on the same port.

**Solution**: `SO_REUSEADDR` tells the kernel "I know what I'm doing, let me bind to this port even if it's in TIME_WAIT." This is safe for servers because TIME_WAIT only affects the specific (srcIP, srcPort, dstIP, dstPort) 4-tuple, not all connections on the port.

### `SO_REUSEPORT`
**Problem**: Scaling a single-threaded server to multiple cores.

**Solution**: `SO_REUSEPORT` (Linux 3.9+) allows multiple sockets to bind to the same port. The kernel distributes incoming connections across all bound sockets using a hash of the source IP/port (consistent hashing, so connections from the same client go to the same socket).

**Use case**: Run N instances of the server process (one per core), each with its own `epoll` loop and its own accept. The kernel load-balances across them. Nginx uses this architecture.

**Why set it now?**: It doesn't change single-instance behavior, but having the option enabled means we can trivially scale later.

### `TCP_NODELAY`
**Problem**: Nagle's algorithm coalesces small TCP writes into fewer segments to reduce header overhead. This adds up to 40ms of latency for request-response protocols.

**How Nagle works**:
1. First small write: sent immediately.
2. Subsequent small writes: buffered until (a) the outstanding data is ACK'd, or (b) enough data accumulates to fill a segment (MSS, ~1460 bytes).

For a key-value store where responses are often < 100 bytes, Nagle delays every response by the round-trip time. In HFT, this is catastrophic.

**Solution**: `TCP_NODELAY` disables Nagle's algorithm. Every `write()` call immediately generates a TCP segment.

**Trade-off**: More small packets = more header overhead = slightly higher network utilization. For a key-value store on localhost or a datacenter LAN, this overhead is negligible compared to the latency benefit.

---

## Decision 5: Per-Connection Read Buffer Strategy

### Design

```
struct Connection {
    char read_buf[4096];       // Fixed-size, inline buffer
    std::size_t read_offset;   // Valid bytes: [0, read_offset)
};
```

### Why 4096 Bytes?
- Redis commands are typically < 100 bytes (`SET key value`)
- 4KB = 1 memory page, cache-line aligned by most allocators
- 1000 connections × 4KB = 4MB — fits easily in L3 cache
- TCP segments on localhost are ≤ MTU (typically 65,535 for loopback)

### Why a Fixed Array (Not `std::vector`)?
- **No heap allocation**: The buffer is part of the `Connection` struct. When we `emplace` a Connection into the map, the buffer is allocated as part of the map node — one allocation total.
- **Cache-friendly**: The buffer is adjacent to `read_offset` and `fd` in memory. Accessing `conn.read_buf[conn.read_offset]` hits the same cache line.
- **`std::vector` overhead**: 24 bytes of metadata (pointer + size + capacity), plus a separate heap allocation for the data, plus an indirection on every access.

### Handling Partial Reads
TCP is a byte stream, not a message stream. A 50-byte command might arrive as:
- One segment: `[SET key value\r\n]` — easy
- Two segments: `[SET ke]` then `[y value\r\n]` — must accumulate
- One segment with two commands: `[PING\r\nGET key\r\n]` — must parse both

The `read_offset` field tracks accumulated bytes. In Layer 2, the protocol parser will:
1. Scan `[0, read_offset)` for complete commands
2. Parse and execute complete commands
3. Shift remaining partial data to the front: `memmove(buf, buf + consumed, remaining)`

---

## Decision 6: Connection Lifecycle

```
accept4() → read_buf initialized → epoll_add(EPOLLIN|EPOLLET)
    ↓
EPOLLIN → read() loop until EAGAIN → invoke callback → epoll_mod(+EPOLLOUT)
    ↓
EPOLLOUT → write() loop until EAGAIN → epoll_mod(-EPOLLOUT)
    ↓
read() returns 0 (EOF) or EPOLLERR/EPOLLHUP → close(fd) → erase from map
```

### Key Design Points

1. **EPOLLOUT is registered on-demand**: We only add EPOLLOUT when `write_buf` is non-empty. If we always registered it, `epoll_wait()` would return constantly because the send buffer is almost always writable (it's 128KB+ on Linux). This would cause a 100% CPU busy-spin.

2. **Read before write in the event dispatch**: When both EPOLLIN and EPOLLOUT fire on the same fd, we read first because the read callback may generate response data that we want to write immediately.

3. **Deletion check after read**: `handle_read` may close the connection (on EOF or error). The event loop checks `connections_.count(fd)` before calling `handle_write` to avoid a use-after-free.

4. **No explicit `EPOLL_CTL_DEL`**: When `close(fd)` is called, the kernel automatically removes the fd from all epoll instances. This is guaranteed since Linux 2.6.9 (as long as no other fd refers to the same underlying file description via `dup`).

---

## Decision 7: Error Handling Strategy

### Approach: Error Codes + Logging, No Exceptions

1. **Setup functions** return `std::string` (empty = success, non-empty = error message). The caller decides whether to retry or abort.

2. **Event handlers** log errors to `stderr` and close the offending connection. The server never crashes due to a single bad client.

3. **Every syscall** is checked:
   - `socket()`, `bind()`, `listen()`, `epoll_create1()`, `accept4()`: Return -1 on error
   - `read()`, `write()`: Return -1 on error, 0 on EOF
   - `setsockopt()`, `fcntl()`, `epoll_ctl()`: Return -1 on error

4. **`EINTR` handling**: Signal-interrupted syscalls are retried immediately. This is critical for servers that handle signals (e.g., SIGHUP for config reload).

### Why Not Exceptions?
- Exception unwinding is expensive (~500ns per throw on x86) and non-deterministic (it walks the stack)
- In latency-sensitive code, you need bounded worst-case time per operation
- Error codes give the caller explicit control over the error path
- In Layer 2+, we'll use `std::expected<T, E>` (C++23) or a similar pattern for richer error types

---

## Decision 8: `SIGPIPE` Handling

### The Problem
When we `write()` to a socket whose peer has closed:
1. The kernel delivers `SIGPIPE` to our process
2. The default handler terminates the process
3. **One dead client kills the entire server**

### The Fix
```cpp
signal(SIGPIPE, SIG_IGN);
```

Now `write()` returns -1 with `errno = EPIPE` instead of killing us. We handle this gracefully in `handle_write` by closing the connection.

### Alternative
We could use `send()` with `MSG_NOSIGNAL` instead of `write()`. This suppresses SIGPIPE per-call rather than globally. However, setting `SIG_IGN` globally is simpler and covers all write paths.

---

## Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| Connections per thread | ~10,000+ | Limited by fd limit (`ulimit -n`), not by CPU |
| Accept latency | ~1μs | `accept4` is a single syscall |
| Read path syscalls | 2 per event | `read()` + `read()` returning EAGAIN |
| Write path syscalls | 2 per drain | `write()` + `write()` returning EAGAIN |
| Memory per connection | ~4.2 KB | 4096 read buffer + 24 bytes overhead |
| Event loop overhead | O(active) | Only active connections generate events (ET) |

---

## What Layer 2 Changes

In Layer 2, we replace the echo callback with a RESP protocol parser that:
1. Scans the read buffer for complete commands (terminated by `\r\n`)
2. Parses commands into (command, args) tuples using `std::string_view` (zero-copy)
3. Dispatches to command handlers (PING, GET, SET, DEL)
4. Formats RESP responses into the write buffer

The server infrastructure (epoll, connection management, buffer handling) remains unchanged — it's designed to be protocol-agnostic.
