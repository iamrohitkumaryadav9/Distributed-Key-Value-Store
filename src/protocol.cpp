// ─────────────────────────────────────────────────────────────────────────────
// protocol.cpp — RESP inline protocol parser and command handler
//
// Implementation notes:
//   - parse_inline_command: Scans for \n, strips optional \r, splits by space.
//     All operations use pointer arithmetic on string_view — no allocation.
//   - Response formatters: Append RESP-formatted text to the output string.
//     Length conversion uses std::to_chars (stack-allocated buffer).
//   - dispatch_command: Case-insensitive command matching without allocation.
// ─────────────────────────────────────────────────────────────────────────────

#include "protocol.h"
#include "replication.h"    // ReplicationManager for broadcasting
#include "wal.h"            // WAL for dispatch_command WAL-first writes

#include <algorithm>    // std::min (not used currently, but included for safety)
#include <cctype>       // std::toupper
#include <charconv>     // std::to_chars — zero-allocation integer-to-string
#include <cstring>      // memcmp (if needed)

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Case-insensitive string comparison
// ─────────────────────────────────────────────────────────────────────────────
// Redis commands are case-insensitive: "ping", "PING", "Ping" all work.
// We compare character-by-character with toupper() instead of creating
// a lowercase/uppercase copy of the command string.
//
// Why not std::ranges::equal with a projection?
//   It would work, but it's less explicit and harder to reason about
//   performance. The manual loop makes the O(n) cost obvious and avoids
//   pulling in <ranges>.
//
// Note: static linkage (file-local) — this helper is an implementation
// detail, not part of the public API.

static bool str_iequal(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        // Cast to unsigned char before toupper: the C standard says
        // toupper(c) is undefined if c is not representable as unsigned char
        // and is not EOF. Signed char values > 127 would be negative ints,
        // causing undefined behavior. This cast prevents that.
        if (std::toupper(static_cast<unsigned char>(a[i])) !=
            std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// parse_inline_command
// ─────────────────────────────────────────────────────────────────────────────

ParseResult parse_inline_command(std::string_view buffer) {
    // ── Step 1: Find the line terminator ─────────────────────────────────
    // RESP protocol uses \r\n, but we also accept bare \n for compatibility
    // with tools like netcat that send \n only. This makes interactive
    // testing much easier without sacrificing correctness.
    //
    // We search for \n (which catches both \r\n and \n) rather than
    // searching for \r\n specifically. If we find \n, we strip a
    // preceding \r if present.
    auto newline_pos = buffer.find('\n');

    if (newline_pos == std::string_view::npos) {
        // No complete line yet. The caller should retain the buffer
        // contents and append more data from the next TCP read.
        // bytes_consumed = 0 signals "I consumed nothing, keep everything."
        ParseResult result;
        result.status = ParseStatus::kIncomplete;
        result.bytes_consumed = 0;
        return result;
    }

    // ── Step 2: Extract the command line ─────────────────────────────────
    // The command is everything before the \n, minus an optional trailing \r.
    std::string_view line = buffer.substr(0, newline_pos);

    // Strip trailing \r if present (handles \r\n termination)
    if (!line.empty() && line.back() == '\r') {
        line.remove_suffix(1);
    }

    // Bytes consumed includes everything up to and including the \n.
    // The caller will advance its read pointer by this amount.
    std::size_t consumed = newline_pos + 1;

    // ── Step 3: Handle empty lines ───────────────────────────────────────
    // An empty line (just \r\n or \n) is not an error — Redis silently
    // ignores it. Telnet and netcat often send empty lines.
    if (line.empty()) {
        ParseResult result;
        result.status = ParseStatus::kComplete;
        result.bytes_consumed = consumed;
        result.num_args = 0;
        return result;
    }

    // ── Step 4: Tokenize by spaces ───────────────────────────────────────
    // Split the line into space-separated tokens. Each token becomes a
    // string_view pointing into the original buffer — zero copy.
    //
    // We handle multiple consecutive spaces by skipping them. Redis does
    // the same: "SET  key  value" works identically to "SET key value".
    ParseResult result;
    result.status = ParseStatus::kComplete;
    result.bytes_consumed = consumed;
    result.num_args = 0;

    std::size_t i = 0;
    while (i < line.size()) {
        // Skip leading whitespace before the next token
        while (i < line.size() && line[i] == ' ') {
            ++i;
        }

        // If we've reached the end after skipping spaces, we're done
        if (i >= line.size()) break;

        // Check if we've exceeded our argument limit
        if (result.num_args >= ParseResult::kMaxArgs) {
            // Too many arguments. This could be a malformed command or
            // a command we don't support. Return an error so the caller
            // can send an appropriate RESP error response.
            result.status = ParseStatus::kError;
            return result;
        }

        // Find the end of this token (next space or end of line)
        std::size_t token_start = i;
        while (i < line.size() && line[i] != ' ') {
            ++i;
        }

        // Store the token as a string_view into the original buffer.
        // This is the key zero-copy optimization: we never construct a
        // std::string from the token. The string_view is just a
        // {pointer, length} pair pointing into the read buffer.
        result.args[result.num_args] = line.substr(token_start, i - token_start);
        ++result.num_args;
    }

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// RESP Response Formatters
// ─────────────────────────────────────────────────────────────────────────────

void resp_simple_string(std::string& out, std::string_view msg) {
    // Format: +<msg>\r\n
    // Reserve space to avoid multiple reallocations.
    // 1 ('+') + msg.size() + 2 ("\r\n") = msg.size() + 3
    out.reserve(out.size() + msg.size() + 3);
    out += '+';
    out.append(msg.data(), msg.size());
    out += '\r';
    out += '\n';
}

void resp_error(std::string& out, std::string_view msg) {
    // Format: -<msg>\r\n
    out.reserve(out.size() + msg.size() + 3);
    out += '-';
    out.append(msg.data(), msg.size());
    out += '\r';
    out += '\n';
}

void resp_bulk_string(std::string& out, std::string_view data) {
    // Format: $<length>\r\n<data>\r\n
    //
    // The length prefix is what distinguishes RESP from plain-text protocols.
    // The client reads the length first, allocates a buffer of exactly that
    // size, then reads exactly that many bytes. No scanning for delimiters,
    // no ambiguity with binary data that contains \r\n.

    // Convert the length to a string using std::to_chars.
    // std::to_chars writes directly into our stack buffer — no heap
    // allocation, no locale dependency. It's the fastest way to convert
    // an integer to a string in C++17/20.
    //
    // 20 chars is enough for any 64-bit integer (max 20 digits).
    char len_buf[20];
    auto [ptr, ec] = std::to_chars(len_buf, len_buf + sizeof(len_buf),
                                   data.size());
    // to_chars cannot fail here: the buffer is large enough for any size_t,
    // and size_t is always a valid integer. But we check anyway because
    // "every return value checked" is a non-negotiable.
    if (ec != std::errc{}) {
        // This should be unreachable. If it fires, something is very wrong.
        resp_error(out, "ERR internal: to_chars failed");
        return;
    }

    std::string_view len_str(len_buf, static_cast<std::size_t>(ptr - len_buf));

    // Pre-calculate total size: $, length digits, \r\n, data, \r\n
    out.reserve(out.size() + 1 + len_str.size() + 2 + data.size() + 2);
    out += '$';
    out.append(len_str.data(), len_str.size());
    out += '\r';
    out += '\n';
    out.append(data.data(), data.size());
    out += '\r';
    out += '\n';
}

void resp_nil(std::string& out) {
    // Format: $-1\r\n
    // This is a special bulk string with length -1, meaning "no data."
    // It's different from an empty string "$0\r\n\r\n" — nil means
    // "the key does not exist", empty means "the key exists with no value."
    out.append("$-1\r\n", 5);
}

void resp_integer(std::string& out, int64_t value) {
    // Format: :<value>\r\n
    char val_buf[21];  // 20 digits + sign for int64_t
    auto [ptr, ec] = std::to_chars(val_buf, val_buf + sizeof(val_buf), value);
    if (ec != std::errc{}) {
        resp_error(out, "ERR internal: to_chars failed");
        return;
    }

    std::string_view val_str(val_buf, static_cast<std::size_t>(ptr - val_buf));

    out.reserve(out.size() + 1 + val_str.size() + 2);
    out += ':';
    out.append(val_str.data(), val_str.size());
    out += '\r';
    out += '\n';
}

// ─────────────────────────────────────────────────────────────────────────────
// dispatch_command — Execute a parsed command against the store
// ─────────────────────────────────────────────────────────────────────────────

void dispatch_command(const ParseResult& cmd, std::string& out,
                      Store<>& store, WAL* wal,
                      ReplicationManager* repl, bool read_only) {
    // Empty command (blank line) — silently ignore.
    // This happens when the client sends \r\n with no preceding text.
    if (cmd.num_args == 0) {
        return;
    }

    // The command name is always the first argument.
    std::string_view command = cmd.args[0];

    // ── PING ────────────────────────────────────────────────────────────
    // The simplest command: returns +PONG\r\n.
    // Used by clients to test connectivity and measure round-trip latency.
    // PING is a read-only command — no WAL write needed.
    if (str_iequal(command, "PING")) {
        if (cmd.num_args > 2) {
            resp_error(out, "ERR wrong number of arguments for 'PING' command");
            return;
        }
        if (cmd.num_args == 2) {
            // PING with a message: echo the message back as a bulk string.
            // Example: PING hello → $5\r\nhello\r\n
            resp_bulk_string(out, cmd.args[1]);
        } else {
            // PING without arguments: simple +PONG\r\n
            resp_simple_string(out, "PONG");
        }
        return;
    }

    // ── GET <key> ───────────────────────────────────────────────────────
    // Returns the value associated with the key, or nil if not found.
    // GET is a read-only command — no WAL write needed.
    // Replicas CAN serve GET requests (read-only).
    if (str_iequal(command, "GET")) {
        if (cmd.num_args != 2) {
            resp_error(out, "ERR wrong number of arguments for 'GET' command");
            return;
        }

        auto value = store.get(cmd.args[1]);
        if (value.has_value()) {
            // Key exists: return the value as a bulk string.
            resp_bulk_string(out, *value);
        } else {
            // Key not found (or expired): return nil ($-1\r\n).
            resp_nil(out);
        }
        return;
    }

    // ── SET <key> <value> ───────────────────────────────────────────────
    // Stores the key-value pair, overwriting any existing value.
    //
    // Read-only check (Layer 5):
    //   Replicas reject writes — they receive data from the leader.
    //
    // WAL-first protocol (Layer 4):
    //   1. Write SET entry to WAL (durable)
    //   2. If WAL write succeeds, broadcast to replicas (Layer 5)
    //   3. Update the in-memory store
    //   4. If WAL write fails, return error WITHOUT modifying the store
    if (str_iequal(command, "SET")) {
        if (cmd.num_args != 3) {
            resp_error(out, "ERR wrong number of arguments for 'SET' command");
            return;
        }

        // Replica guard: reject writes on read-only replicas
        if (read_only) {
            resp_error(out, "READONLY You can't write against a read-only replica");
            return;
        }

        // WAL-first: write to durable log before mutating in-memory state.
        // Capture serialized bytes for replication broadcasting.
        if (wal != nullptr) {
            std::vector<uint8_t> wal_bytes;
            if (!wal->append_set(cmd.args[1], cmd.args[2],
                                 repl ? &wal_bytes : nullptr)) {
                resp_error(out, "ERR WAL write failed — data not persisted");
                return;
            }
            // Broadcast the WAL entry to all connected replicas
            if (repl != nullptr && !wal_bytes.empty()) {
                repl->broadcast(wal_bytes.data(),
                                static_cast<uint32_t>(wal_bytes.size()));
            }
        }

        store.set(cmd.args[1], cmd.args[2]);
        resp_simple_string(out, "OK");
        return;
    }

    // ── DEL <key> ───────────────────────────────────────────────────────
    // Deletes the key. Returns :1 if the key existed, :0 otherwise.
    //
    // WAL-first + replication: same protocol as SET.
    // We log the DEL even if the key doesn't exist. This is intentional:
    //   - On replay, DEL of a nonexistent key is a no-op (harmless)
    //   - Checking existence first would require a read lock, adding latency
    //   - Redis also doesn't check existence before logging DEL
    if (str_iequal(command, "DEL")) {
        if (cmd.num_args != 2) {
            resp_error(out, "ERR wrong number of arguments for 'DEL' command");
            return;
        }

        // Replica guard: reject writes on read-only replicas
        if (read_only) {
            resp_error(out, "READONLY You can't write against a read-only replica");
            return;
        }

        if (wal != nullptr) {
            std::vector<uint8_t> wal_bytes;
            if (!wal->append_del(cmd.args[1],
                                 repl ? &wal_bytes : nullptr)) {
                resp_error(out, "ERR WAL write failed — data not persisted");
                return;
            }
            if (repl != nullptr && !wal_bytes.empty()) {
                repl->broadcast(wal_bytes.data(),
                                static_cast<uint32_t>(wal_bytes.size()));
            }
        }

        bool deleted = store.del(cmd.args[1]);
        resp_integer(out, deleted ? 1 : 0);
        return;
    }

    // ── Unknown command ─────────────────────────────────────────────────
    // Build a descriptive error message including the command name.
    // This helps the user identify typos (e.g., "SEET" instead of "SET").
    std::string err_msg = "ERR unknown command '";
    err_msg.append(command.data(), command.size());
    err_msg += '\'';
    resp_error(out, err_msg);
}
