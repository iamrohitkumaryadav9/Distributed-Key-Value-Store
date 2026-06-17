#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// protocol.h — RESP inline protocol parser and command handler
//
// Design principles:
//   1. Zero dynamic allocation during parsing — all string_views reference
//      the caller's read buffer directly (zero-copy).
//   2. Resumable parsing — if a command is split across TCP segments,
//      the parser returns kIncomplete and the caller retains the partial
//      data in the buffer for the next read.
//   3. Fixed-size ParseResult — no heap allocation, lives on the stack.
//   4. RESP response formatting — small helper functions that append
//      formatted RESP responses directly to an output buffer.
//
// RESP (REdis Serialization Protocol) inline format:
//   Commands are space-separated tokens terminated by \r\n.
//   Example: "SET mykey myvalue\r\n"
//
//   Response types:
//     Simple String: +OK\r\n
//     Error:         -ERR message\r\n
//     Integer:       :42\r\n
//     Bulk String:   $5\r\nhello\r\n
//     Nil:           $-1\r\n
// ─────────────────────────────────────────────────────────────────────────────

#include <cstddef>          // std::size_t
#include <cstdint>          // uint8_t, int64_t
#include <string>           // std::string
#include <string_view>      // std::string_view

#include "store.h"          // Store<> for dispatch_command

// Forward declarations to avoid circular includes.
// WAL and ReplicationManager are only needed by dispatch_command.
class WAL;
class ReplicationManager;

// ─────────────────────────────────────────────────────────────────────────────
// Parse Status
// ─────────────────────────────────────────────────────────────────────────────
// Using uint8_t as the underlying type to minimize ParseResult size.
// The enum has only 3 values, so 1 byte is sufficient.

enum class ParseStatus : uint8_t {
    kComplete,    // A complete command was successfully parsed
    kIncomplete,  // Need more data — no \r\n (or \n) found yet
    kError        // Malformed input (e.g., too many arguments)
};

// ─────────────────────────────────────────────────────────────────────────────
// ParseResult — The output of parsing one inline RESP command
// ─────────────────────────────────────────────────────────────────────────────
// This struct is designed to live on the stack (no heap allocation).
// All string_views in args[] point into the caller's read buffer — they
// are valid only as long as the read buffer is not modified or freed.
//
// Layout (x86_64, 8-byte alignment):
//   offset 0:  status          (1 byte)
//   offset 1:  [7 bytes padding to align bytes_consumed]
//   offset 8:  bytes_consumed  (8 bytes)
//   offset 16: num_args        (8 bytes)
//   offset 24: args[0..3]      (4 × 16 = 64 bytes)
//   Total: 88 bytes = 1.375 cache lines

struct ParseResult {
    // Maximum number of arguments we support in a single command.
    // PING=1, GET=2, SET=3, DEL=2. 4 gives headroom for future commands
    // (e.g., SETEX key seconds value = 4 args).
    static constexpr std::size_t kMaxArgs = 4;

    ParseStatus status = ParseStatus::kIncomplete;

    // Number of bytes consumed from the input buffer, including the
    // terminating \r\n (or \n). The caller should advance its read
    // pointer by this amount after processing the result.
    std::size_t bytes_consumed = 0;

    // Number of valid entries in args[]. Always <= kMaxArgs.
    std::size_t num_args = 0;

    // Parsed arguments as string_views into the original buffer.
    // args[0] is the command name, args[1..] are the arguments.
    // These are zero-copy references — no strings are constructed.
    std::string_view args[kMaxArgs] = {};
};

// Verify struct size matches our expected layout.
// If this fires, the compiler is padding differently than expected.
// Fix by reordering fields or adding explicit padding.
static_assert(sizeof(ParseResult) == 88,
    "ParseResult must be exactly 88 bytes — check field alignment");

// Verify string_view is 16 bytes (pointer + size) as expected.
// If this fails, the platform has an unusual string_view implementation.
static_assert(sizeof(std::string_view) == 16,
    "std::string_view must be 16 bytes (8-byte pointer + 8-byte size)");

// ─────────────────────────────────────────────────────────────────────────────
// Parser
// ─────────────────────────────────────────────────────────────────────────────

// Parse one inline RESP command from the buffer.
//
// Protocol:
//   Scans for a line terminator (\r\n or \n). If found, splits the line
//   into space-separated tokens and stores them as string_views.
//
// Returns:
//   - kComplete:   A command was parsed. bytes_consumed tells the caller
//                  how many bytes to skip. args[0..num_args) are valid.
//   - kIncomplete: No line terminator found. bytes_consumed = 0.
//                  The caller should retain the buffer and append more
//                  data from the next read.
//   - kError:      The line had too many arguments (> kMaxArgs).
//                  bytes_consumed points past the line so the caller
//                  can skip it.
//
// Zero-copy guarantee:
//   All string_views in the result point into `buffer`. No std::string
//   objects are created. No heap allocation occurs.
//
// Thread safety:
//   This function is stateless and safe to call from any thread.
//   The string_views are valid as long as the underlying buffer is alive.
ParseResult parse_inline_command(std::string_view buffer);

// ─────────────────────────────────────────────────────────────────────────────
// RESP Response Formatters
// ─────────────────────────────────────────────────────────────────────────────
// Each function appends a RESP-formatted response to `out`.
// These are used by the command handler to build responses.
//
// Why append to std::string& instead of returning std::string?
//   - Avoids temporary string construction and move/copy overhead.
//   - Multiple commands in a batch can append to the same write buffer.
//   - The caller (server) already owns the write buffer.

// Simple string: +<msg>\r\n
// Used for: PONG, OK responses.
void resp_simple_string(std::string& out, std::string_view msg);

// Error: -<msg>\r\n
// Used for: ERR unknown command, ERR wrong number of arguments.
void resp_error(std::string& out, std::string_view msg);

// Bulk string: $<len>\r\n<data>\r\n
// Used for: GET responses when the key exists.
// The length prefix allows the client to pre-allocate a buffer before
// reading the data — this is why RESP uses length-prefixed strings
// instead of delimiter-terminated strings.
void resp_bulk_string(std::string& out, std::string_view data);

// Nil: $-1\r\n
// Used for: GET responses when the key does not exist.
// A bulk string with length -1 signals "no data" without ambiguity
// (an empty string "$0\r\n\r\n" is different from nil "$-1\r\n").
void resp_nil(std::string& out);

// Integer: :<value>\r\n
// Used for: DEL response (number of keys deleted: 0 or 1).
void resp_integer(std::string& out, int64_t value);

// ─────────────────────────────────────────────────────────────────────────────
// Command Dispatcher
// ─────────────────────────────────────────────────────────────────────────────
// Takes a parsed command and executes it against the store, appending the
// RESP response to the output buffer.
//
// Supported commands:
//   PING         → +PONG\r\n
//   GET key      → $<len>\r\n<value>\r\n  or  $-1\r\n
//   SET key val  → +OK\r\n
//   DEL key      → :<0|1>\r\n
//   (unknown)    → -ERR unknown command '<cmd>'\r\n
//   (wrong args) → -ERR wrong number of arguments for '<cmd>' command\r\n
//
// Commands are case-insensitive (Redis convention).
//
// WAL-first protocol (Layer 4):
//   When wal is non-null, mutating commands (SET, DEL) write to the WAL
//   BEFORE updating the in-memory store. If the WAL write fails, the
//   store is NOT modified and an error is returned to the client.
//   This guarantees crash consistency: if the server ACKs a write,
//   the write is on disk. On crash recovery, replaying the WAL
//   rebuilds the exact state that was ACK'd to clients.
//
// Replication (Layer 5):
//   When repl is non-null (leader mode), WAL entry bytes are captured
//   during the WAL write and broadcast to all connected replicas.
//
//   When read_only is true (replica mode), mutating commands (SET, DEL)
//   are rejected with a READONLY error. The replica receives writes
//   from the leader's replication stream, not from clients.

void dispatch_command(const ParseResult& cmd, std::string& out,
                      Store<>& store, WAL* wal = nullptr,
                      ReplicationManager* repl = nullptr,
                      bool read_only = false);
