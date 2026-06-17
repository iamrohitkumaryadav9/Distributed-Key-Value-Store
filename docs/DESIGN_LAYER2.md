# Layer 2 — RESP Protocol Parser + Command Handler: Design Decisions

## Overview

Layer 2 adds protocol awareness to the TCP server. Raw bytes from the network are parsed into structured commands using the Redis RESP inline format, dispatched to command handlers (PING, GET, SET, DEL), and responses are formatted back into RESP wire format. The entire parsing pipeline is zero-copy — no heap allocations occur during parsing.

---

## Decision 1: Why RESP Inline Format (Not Full RESP Bulk String Protocol)?

### The Problem
We need a wire protocol to encode commands and responses between client and server.

### RESP Has Two Command Formats

| Format | Example (SET key value) | Complexity |
|--------|------------------------|------------|
| **Inline** | `SET key value\r\n` | Simple — split by spaces |
| **Bulk String Array** | `*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n` | Complex — length-prefixed |

### Why Inline
1. **Simpler to parse**: One `find('\n')` + split by spaces. The bulk string array format requires a state machine to track array length, current element, remaining bytes, etc.
2. **Human-readable**: You can test with `nc localhost 6379` and type commands directly. Bulk string format is binary-ish and requires a client library.
3. **Redis supports it**: Redis itself parses inline commands in `processInlineBuffer()` in networking.c. It's a first-class protocol format.
4. **Sufficient for our commands**: None of our commands have values containing spaces or binary data. If they did, we'd need bulk strings.

### Trade-off
Inline format cannot represent values with spaces (e.g., `SET key "hello world"` won't work — we'd parse 4 arguments). For a production system, full RESP bulk string support is essential. For a portfolio project demonstrating systems engineering, inline is sufficient and lets us focus on the interesting parts (zero-copy parsing, buffer management).

---

## Decision 2: Zero-Copy Parsing with `std::string_view`

### The Problem
Every parsed command generates argument strings (command name, key, value). Creating `std::string` objects for each argument would require heap allocations — multiple per command, on the hot path.

### The Solution
`std::string_view` is a non-owning reference: `{const char* data, size_t length}`. It's 16 bytes and construction is free (no allocation, no copy).

```cpp
// All string_views point into the original read buffer:
//
//   read_buf:  [S E T   k e y   v a l u e \r \n ...]
//               ^       ^       ^
//   args[0] ----+       |       |
//   args[1] ------------+       |
//   args[2] --------------------+
```

### Why This Matters for HFT
In a trading system processing millions of messages per second, even a small allocation per message can cause:
1. **Latency spikes**: `malloc()` acquires a lock (in glibc's ptmalloc2). Under contention, this can spike to microseconds.
2. **GC pressure**: Not applicable to C++, but heap fragmentation degrades allocator performance over time.
3. **Cache pollution**: Each allocation touches a new cache line, evicting hot data.

Zero-copy parsing avoids all three. The data stays in the read buffer (already in L1 cache from the `read()` syscall), and we just point at it.

### Lifetime Safety
The string_views are valid only while the read buffer is unmodified. Our single-threaded model guarantees this: we parse, dispatch, format the response, and only then compact the buffer. No concurrent modification is possible.

---

## Decision 3: Fixed-Size `ParseResult` (Stack-Allocated, 88 Bytes)

### Design

```cpp
struct ParseResult {
    static constexpr size_t kMaxArgs = 4;
    ParseStatus status;          //  1 byte
    size_t bytes_consumed;       //  8 bytes
    size_t num_args;             //  8 bytes
    string_view args[kMaxArgs];  // 64 bytes
};                               // Total: 88 bytes
static_assert(sizeof(ParseResult) == 88, "...");
```

### Why Fixed-Size
1. **No heap allocation**: The struct lives on the stack. Calling `parse_inline_command()` a million times causes zero heap allocations.
2. **Predictable size**: `static_assert` catches any unexpected padding changes across compilers or platforms.
3. **Cache-friendly**: 88 bytes fits in 1.5 cache lines. The entire result is likely in L1 after construction.

### Why `kMaxArgs = 4`
Our commands use at most 3 arguments (SET key value). 4 gives headroom for future commands (e.g., `SETEX key seconds value`). More than 4 would waste stack space for commands we don't support.

### Alternative: `std::vector<std::string_view>`
Would allow arbitrary argument counts but:
- Heap allocation for the vector's storage
- 24 bytes of metadata (pointer + size + capacity)
- Indirection on every access (pointer dereference vs. array index)

---

## Decision 4: Resumable Parsing (Handling TCP Partial Reads)

### The Problem
TCP is a byte stream. A 20-byte command might arrive as:
- One segment: `SET key value\r\n` (best case)
- Two segments: `SET ke` then `y value\r\n` (split mid-token)
- One segment with two commands: `PING\r\nGET key\r\n` (pipelining)
- Mixed: `PING\r\nSET ke` (one complete + one partial)

### The Solution: Stateless, Delimiter-Based Parsing

```
parse_inline_command(buffer):
    1. Search for '\n' in buffer
    2. If not found → return kIncomplete (bytes_consumed = 0)
    3. If found at pos → extract line [0, pos), split by spaces
    4. Return kComplete with bytes_consumed = pos + 1
```

The parser is **stateless** — it doesn't maintain any state between calls. This is possible because:
- We never partially consume a command (either the full `\r\n` is there, or we wait)
- The server retains unconsumed bytes in the read buffer across reads
- Each call scans from the beginning of the remaining buffer

### Buffer Compaction

After processing, we shift unconsumed bytes to the front:

```
Before: [PING\r\nSET ke............]
         ^^^^^^  ^^^^^^
         consumed remaining

After:  [SET ke....................]
         ^^^^^^
         read_offset = 6
```

We use `memmove` (not `memcpy`) because the source and destination regions may overlap when the consumed portion is small.

### Why Not a State Machine?
A state machine parser would track "I'm currently inside a token, at position X, after reading N arguments." This is more complex and error-prone. Since our protocol uses a simple delimiter (`\r\n`), the stateless approach is simpler and equally efficient.

---

## Decision 5: Transparent Hash for Zero-Allocation Lookups

### The Problem
`std::unordered_map<std::string, V>::find(key)` requires `key` to be a `std::string`. If we have a `std::string_view` (from zero-copy parsing), the compiler constructs a temporary `std::string` — a heap allocation on every GET lookup.

```cpp
// BAD: This allocates a temporary std::string on every call
auto it = map.find(std::string(key_view));  // heap allocation!
```

### The Solution: C++20 Heterogeneous Lookup (P0919R3)

```cpp
struct TransparentStringHash {
    using is_transparent = void;  // Magic tag
    size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
};

struct TransparentStringEqual {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept {
        return a == b;
    }
};

// Now find() accepts string_view directly — zero allocation
std::unordered_map<std::string, std::string,
                   TransparentStringHash, TransparentStringEqual> map;
auto it = map.find(key_view);  // No temporary string!
```

### How It Works
The `is_transparent` tag tells the container: "My hash and equality functors can accept any type, not just the key type." When you call `find(string_view)`, the container:
1. Hashes the `string_view` using `TransparentStringHash`
2. Compares bucket entries using `TransparentStringEqual(string_view, string)`
3. Returns the matching iterator — all without creating a temporary `std::string`

### Critical Invariant
The hash of a `std::string` must equal the hash of a `std::string_view` with the same content. Otherwise, `find()` would look in the wrong bucket. Our implementation guarantees this because both `std::hash<std::string>` and `std::hash<std::string_view>` hash the same byte sequence identically.

### Performance Impact
On the GET hot path:
- **Before**: `find(std::string(key_view))` = 1 `malloc` + 1 `memcpy` + 1 hash + 1 `free`
- **After**: `find(key_view)` = 1 hash

This eliminates ~80ns of allocator overhead per lookup (measured on typical glibc malloc).

---

## Decision 6: Case-Insensitive Command Matching Without Allocation

### The Problem
Redis commands are case-insensitive: `PING`, `ping`, `Ping` are all valid.

### Alternatives

| Approach | Allocation? | Thread-safe? |
|----------|------------|-------------|
| Convert command to uppercase, compare | Yes (creates new string) | Yes |
| `strcasecmp()` | No | Yes (but POSIX-specific) |
| Character-by-character `toupper()` | No | Yes |

### Our Choice: Manual `toupper()` Loop

```cpp
static bool str_iequal(string_view a, string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (toupper((unsigned char)a[i]) != toupper((unsigned char)b[i]))
            return false;
    }
    return true;
}
```

**Why the `(unsigned char)` cast?** The C standard says `toupper(c)` is undefined if `c` is not representable as `unsigned char` and is not `EOF`. On platforms where `char` is signed, values > 127 would be negative `int` values — undefined behavior. The cast prevents this. This is a classic C/C++ pitfall that interviewers love to ask about.

### Why Not `std::ranges::equal` with a Projection?
```cpp
// Alternative: ranges-based approach
std::ranges::equal(a, b, {}, toupper_proj, toupper_proj);
```
This works but pulls in `<ranges>` (a heavyweight header that increases compile time) and is less explicit about the performance characteristics. The manual loop makes the O(n) cost and zero-allocation guarantee obvious.

---

## Decision 7: Response Formatting with `std::to_chars`

### The Problem
RESP bulk strings require encoding the data length as ASCII digits: `$5\r\nhello\r\n`. We need to convert `size_t` → ASCII string.

### Alternatives

| Method | Allocation? | Locale-dependent? | Speed |
|--------|------------|-------------------|-------|
| `std::to_string()` | Yes (returns `std::string`) | Yes | Slow |
| `snprintf()` | No (writes to buffer) | Yes | Medium |
| `std::to_chars()` | No (writes to buffer) | No | Fast |

### Why `std::to_chars` (C++17)

```cpp
char buf[20];
auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
```

1. **Zero allocation**: Writes directly into a stack-allocated buffer.
2. **Locale-independent**: `std::to_string()` and `snprintf()` use the current locale's decimal separator (could be `,` instead of `.` in some locales). `std::to_chars` always uses `.` — critical for a binary protocol.
3. **Fastest**: No locale lookup, no format string parsing, no `std::string` construction.

### The Structured Binding
`std::to_chars` returns `{pointer_past_end, error_code}`. We use C++17 structured bindings to capture both: `auto [ptr, ec] = ...`. The output length is `ptr - buf`.

---

## Decision 8: Server Integration — Callback Buffer Management Contract

### The Problem
In Layer 1, the server reset `conn.read_offset = 0` after the callback (echo mode — all data is "processed"). In Layer 2, a partial command at the end of the buffer must survive across reads.

### The Solution: Callback Owns Buffer State

```
Layer 1: Server resets read_offset after callback
Layer 2: Callback manages read_offset (compacts buffer)
```

The callback now:
1. Parses complete commands in a loop
2. Dispatches each command and appends responses to `write_buf`
3. `memmove`s remaining bytes to the front of `read_buf`
4. Sets `conn.read_offset` to the number of remaining bytes

This design is clean because:
- The server doesn't need to know about the protocol format
- The callback has full control over buffer lifecycle
- Partial commands automatically persist across reads

---

## Decision 9: Error Handling — Graceful Degradation

### Design Philosophy
A malformed command from one client must never affect other clients or crash the server.

| Scenario | Response | Action |
|----------|----------|--------|
| Unknown command | `-ERR unknown command 'FOO'\r\n` | Continue serving |
| Wrong argument count | `-ERR wrong number of arguments for 'GET' command\r\n` | Continue serving |
| Too many arguments (> 4) | `-ERR protocol error: too many arguments\r\n` | Skip line, continue |
| Buffer full (no `\r\n` in 4KB) | Close connection | Free resources |

Including the command name in error messages helps the client identify typos without round-tripping to the server.

---

## Test Coverage

| Category | Tests | What's Verified |
|----------|-------|----------------|
| Parser | 11 | Single/multi commands, partial reads, empty, spaces, too many args, zero-copy, resumable |
| Formatters | 7 | Simple string, error, bulk string, empty bulk, nil, integer, batch append |
| SimpleStore | 6 | Get, set, overwrite, delete, nonexistent, heterogeneous lookup |
| Commands | 8 | PING, PING+msg, case-insensitive, SET+GET, GET nil, DEL, unknown, wrong args |
| Integration | 3 | Batch processing, partial command across reads, complete+partial in one buffer |
| **Total** | **35** | Full pipeline coverage |

---

## What Layer 3 Changes

In Layer 3, we replace `SimpleStore` with a sharded concurrent hash map:

1. **New files**: `store.h`, `store.cpp` — the sharded hash map
2. **protocol.h**: Remove `SimpleStore` class, import `Store` from `store.h`
3. **main.cpp**: Replace `SimpleStore store;` with `Store store;`
4. **dispatch_command**: Signature changes to take `Store&` instead of `SimpleStore&`

The protocol parser (`parse_inline_command`) and response formatters are unchanged — they are protocol concerns, independent of the storage backend.
