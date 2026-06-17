// ─────────────────────────────────────────────────────────────────────────────
// test_protocol.cpp — Unit tests for the RESP inline protocol parser
//
// Uses assert() for testing — no test framework dependency.
// If all tests pass, exits with code 0 and prints a summary.
// If any test fails, assert() aborts with file:line info.
//
// Test strategy:
//   1. Parser tests:   Verify zero-copy parsing, partial commands, edge cases
//   2. Formatter tests: Verify RESP response encoding
//   3. Command tests:  Verify dispatch_command for all supported commands
//   4. Integration:    Verify multi-command buffer processing
//
// Note: Store-specific tests (thread safety, TTL, sharding) are in
//       tests/test_store.cpp. This file tests the protocol layer only.
// ─────────────────────────────────────────────────────────────────────────────

#include "protocol.h"

#include <cassert>
#include <cstdio>
#include <cstring>    // memmove
#include <string>
#include <string_view>

// ─────────────────────────────────────────────────────────────────────────────
// Helper: simulate processing a buffer of commands (like the server callback)
// ─────────────────────────────────────────────────────────────────────────────
// This mirrors the logic in main.cpp's message callback, allowing us to test
// the full parse-dispatch-compact pipeline without running the server.

static std::string process_buffer(char* buf, std::size_t& len, Store<>& store) {
    std::string response;
    std::string_view buffer(buf, len);
    std::size_t total_consumed = 0;

    while (!buffer.empty()) {
        ParseResult result = parse_inline_command(buffer);

        if (result.status == ParseStatus::kIncomplete) {
            break;
        }

        if (result.status == ParseStatus::kComplete && result.num_args > 0) {
            dispatch_command(result, response, store);
        } else if (result.status == ParseStatus::kError) {
            resp_error(response, "ERR too many arguments");
        }

        total_consumed += result.bytes_consumed;
        buffer.remove_prefix(result.bytes_consumed);
    }

    // Compact the buffer: shift unconsumed bytes to the front
    std::size_t remaining = len - total_consumed;
    if (remaining > 0 && total_consumed > 0) {
        memmove(buf, buf + total_consumed, remaining);
    }
    len = remaining;

    return response;
}

// ═════════════════════════════════════════════════════════════════════════════
// Parser Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_parse_single_command() {
    // A complete command terminated by \r\n
    std::string_view input = "PING\r\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kComplete);
    assert(r.bytes_consumed == 6);  // "PING\r\n" = 6 bytes
    assert(r.num_args == 1);
    assert(r.args[0] == "PING");

    printf("  [PASS] test_parse_single_command\n");
}

static void test_parse_command_with_args() {
    // SET command with key and value
    std::string_view input = "SET mykey myvalue\r\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kComplete);
    assert(r.bytes_consumed == 19);
    assert(r.num_args == 3);
    assert(r.args[0] == "SET");
    assert(r.args[1] == "mykey");
    assert(r.args[2] == "myvalue");

    printf("  [PASS] test_parse_command_with_args\n");
}

static void test_parse_incomplete_command() {
    // No \r\n — the command is incomplete (split across TCP segments)
    std::string_view input = "SET mykey";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kIncomplete);
    assert(r.bytes_consumed == 0);  // Nothing consumed — keep all data

    printf("  [PASS] test_parse_incomplete_command\n");
}

static void test_parse_empty_buffer() {
    // Empty input — trivially incomplete
    std::string_view input = "";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kIncomplete);
    assert(r.bytes_consumed == 0);

    printf("  [PASS] test_parse_empty_buffer\n");
}

static void test_parse_empty_line() {
    // Just \r\n — an empty command. Should be treated as complete
    // with 0 arguments (silently ignored by dispatch_command).
    std::string_view input = "\r\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kComplete);
    assert(r.bytes_consumed == 2);
    assert(r.num_args == 0);

    printf("  [PASS] test_parse_empty_line\n");
}

static void test_parse_bare_newline() {
    // Some tools (netcat) send \n instead of \r\n.
    // Our parser should handle both.
    std::string_view input = "PING\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kComplete);
    assert(r.bytes_consumed == 5);  // "PING\n" = 5 bytes
    assert(r.num_args == 1);
    assert(r.args[0] == "PING");

    printf("  [PASS] test_parse_bare_newline\n");
}

static void test_parse_extra_spaces() {
    // Multiple consecutive spaces between tokens — should be collapsed
    std::string_view input = "  SET   mykey   myvalue  \r\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kComplete);
    assert(r.num_args == 3);
    assert(r.args[0] == "SET");
    assert(r.args[1] == "mykey");
    assert(r.args[2] == "myvalue");

    printf("  [PASS] test_parse_extra_spaces\n");
}

static void test_parse_multiple_commands_in_buffer() {
    // Two commands in one buffer — parse_inline_command should return
    // only the first one. The caller calls it again for the second.
    std::string_view input = "PING\r\nGET key\r\n";
    ParseResult r1 = parse_inline_command(input);

    assert(r1.status == ParseStatus::kComplete);
    assert(r1.bytes_consumed == 6);
    assert(r1.num_args == 1);
    assert(r1.args[0] == "PING");

    // Parse the second command from the remaining buffer
    std::string_view remaining = input.substr(r1.bytes_consumed);
    ParseResult r2 = parse_inline_command(remaining);

    assert(r2.status == ParseStatus::kComplete);
    assert(r2.bytes_consumed == 9);
    assert(r2.num_args == 2);
    assert(r2.args[0] == "GET");
    assert(r2.args[1] == "key");

    printf("  [PASS] test_parse_multiple_commands_in_buffer\n");
}

static void test_parse_too_many_args() {
    // More than kMaxArgs (4) arguments — should return kError
    std::string_view input = "CMD a b c d e\r\n";
    ParseResult r = parse_inline_command(input);

    assert(r.status == ParseStatus::kError);
    // bytes_consumed should still be set so the caller can skip this line
    assert(r.bytes_consumed > 0);

    printf("  [PASS] test_parse_too_many_args\n");
}

static void test_parse_resumable() {
    // Simulate a command split across two TCP reads.
    // First read: "SET ke" (incomplete)
    // Second read: append "y value\r\n"

    char buf[128];
    std::size_t len = 0;

    // First TCP segment
    const char* seg1 = "SET ke";
    std::size_t seg1_len = 6;
    memcpy(buf + len, seg1, seg1_len);
    len += seg1_len;

    ParseResult r1 = parse_inline_command(std::string_view(buf, len));
    assert(r1.status == ParseStatus::kIncomplete);
    assert(r1.bytes_consumed == 0);

    // Second TCP segment
    const char* seg2 = "y value\r\n";
    std::size_t seg2_len = 9;
    memcpy(buf + len, seg2, seg2_len);
    len += seg2_len;

    ParseResult r2 = parse_inline_command(std::string_view(buf, len));
    assert(r2.status == ParseStatus::kComplete);
    assert(r2.bytes_consumed == 15);  // "SET key value\r\n"
    assert(r2.num_args == 3);
    assert(r2.args[0] == "SET");
    assert(r2.args[1] == "key");
    assert(r2.args[2] == "value");

    printf("  [PASS] test_parse_resumable\n");
}

static void test_parse_zero_copy() {
    // Verify that string_views point into the original buffer,
    // not into some internal copy. This is the zero-copy guarantee.
    char buf[] = "GET mykey\r\n";
    std::string_view input(buf, sizeof(buf) - 1);

    ParseResult r = parse_inline_command(input);
    assert(r.status == ParseStatus::kComplete);

    // args[0] ("GET") should point into buf at offset 0
    assert(r.args[0].data() >= buf);
    assert(r.args[0].data() < buf + sizeof(buf));
    assert(r.args[0].data() == buf);  // "GET" starts at buf[0]

    // args[1] ("mykey") should point into buf at offset 4
    assert(r.args[1].data() == buf + 4);  // "mykey" starts at buf[4]

    printf("  [PASS] test_parse_zero_copy\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Response Formatter Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_resp_simple_string() {
    std::string out;
    resp_simple_string(out, "PONG");
    assert(out == "+PONG\r\n");

    printf("  [PASS] test_resp_simple_string\n");
}

static void test_resp_error() {
    std::string out;
    resp_error(out, "ERR unknown command");
    assert(out == "-ERR unknown command\r\n");

    printf("  [PASS] test_resp_error\n");
}

static void test_resp_bulk_string() {
    std::string out;
    resp_bulk_string(out, "hello");
    assert(out == "$5\r\nhello\r\n");

    printf("  [PASS] test_resp_bulk_string\n");
}

static void test_resp_bulk_string_empty() {
    // Empty string is NOT nil — it's a valid empty value
    std::string out;
    resp_bulk_string(out, "");
    assert(out == "$0\r\n\r\n");

    printf("  [PASS] test_resp_bulk_string_empty\n");
}

static void test_resp_nil() {
    std::string out;
    resp_nil(out);
    assert(out == "$-1\r\n");

    printf("  [PASS] test_resp_nil\n");
}

static void test_resp_integer() {
    {
        std::string out;
        resp_integer(out, 42);
        assert(out == ":42\r\n");
    }
    {
        std::string out;
        resp_integer(out, 0);
        assert(out == ":0\r\n");
    }
    {
        std::string out;
        resp_integer(out, -1);
        assert(out == ":-1\r\n");
    }
    {
        std::string out;
        resp_integer(out, 1);
        assert(out == ":1\r\n");
    }

    printf("  [PASS] test_resp_integer\n");
}

static void test_resp_append_multiple() {
    // Multiple responses appended to the same buffer (batch mode)
    std::string out;
    resp_simple_string(out, "PONG");
    resp_nil(out);
    resp_integer(out, 1);
    assert(out == "+PONG\r\n$-1\r\n:1\r\n");

    printf("  [PASS] test_resp_append_multiple\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Command Dispatch Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_cmd_ping() {
    Store<> store;
    std::string out;

    // Parse and dispatch PING
    ParseResult r = parse_inline_command("PING\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "+PONG\r\n");

    printf("  [PASS] test_cmd_ping\n");
}

static void test_cmd_ping_with_message() {
    Store<> store;
    std::string out;

    ParseResult r = parse_inline_command("PING hello\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "$5\r\nhello\r\n");

    printf("  [PASS] test_cmd_ping_with_message\n");
}

static void test_cmd_ping_case_insensitive() {
    Store<> store;
    std::string out;

    ParseResult r = parse_inline_command("ping\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "+PONG\r\n");

    printf("  [PASS] test_cmd_ping_case_insensitive\n");
}

static void test_cmd_set_get() {
    Store<> store;

    // SET
    {
        std::string out;
        ParseResult r = parse_inline_command("SET mykey myvalue\r\n");
        assert(r.status == ParseStatus::kComplete);
        dispatch_command(r, out, store);
        assert(out == "+OK\r\n");
    }

    // GET
    {
        std::string out;
        ParseResult r = parse_inline_command("GET mykey\r\n");
        assert(r.status == ParseStatus::kComplete);
        dispatch_command(r, out, store);
        assert(out == "$7\r\nmyvalue\r\n");
    }

    printf("  [PASS] test_cmd_set_get\n");
}

static void test_cmd_get_nonexistent() {
    Store<> store;
    std::string out;

    ParseResult r = parse_inline_command("GET nosuchkey\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "$-1\r\n");

    printf("  [PASS] test_cmd_get_nonexistent\n");
}

static void test_cmd_del() {
    Store<> store;
    store.set("key1", "value1");

    // DEL existing key
    {
        std::string out;
        ParseResult r = parse_inline_command("DEL key1\r\n");
        assert(r.status == ParseStatus::kComplete);
        dispatch_command(r, out, store);
        assert(out == ":1\r\n");
    }

    // DEL same key again — should return 0
    {
        std::string out;
        ParseResult r = parse_inline_command("DEL key1\r\n");
        dispatch_command(r, out, store);
        assert(out == ":0\r\n");
    }

    // Verify key is gone
    {
        std::string out;
        ParseResult r = parse_inline_command("GET key1\r\n");
        dispatch_command(r, out, store);
        assert(out == "$-1\r\n");
    }

    printf("  [PASS] test_cmd_del\n");
}

static void test_cmd_unknown() {
    Store<> store;
    std::string out;

    ParseResult r = parse_inline_command("FOOBAR\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "-ERR unknown command 'FOOBAR'\r\n");

    printf("  [PASS] test_cmd_unknown\n");
}

static void test_cmd_wrong_args_get() {
    Store<> store;
    std::string out;

    // GET with no key
    ParseResult r = parse_inline_command("GET\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "-ERR wrong number of arguments for 'GET' command\r\n");

    printf("  [PASS] test_cmd_wrong_args_get\n");
}

static void test_cmd_wrong_args_set() {
    Store<> store;
    std::string out;

    // SET with only a key, no value
    ParseResult r = parse_inline_command("SET onlykey\r\n");
    assert(r.status == ParseStatus::kComplete);
    dispatch_command(r, out, store);
    assert(out == "-ERR wrong number of arguments for 'SET' command\r\n");

    printf("  [PASS] test_cmd_wrong_args_set\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Integration Tests — Multi-command buffer processing
// ═════════════════════════════════════════════════════════════════════════════

static void test_integration_batch_commands() {
    // Simulate receiving multiple commands in a single TCP segment
    Store<> store;
    char buf[256];
    const char* input = "SET key1 value1\r\nSET key2 value2\r\nGET key1\r\nGET key2\r\n";
    std::size_t len = strlen(input);
    memcpy(buf, input, len);

    std::string response = process_buffer(buf, len, store);

    // Should have 4 responses: OK, OK, bulk_string("value1"), bulk_string("value2")
    assert(response == "+OK\r\n+OK\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n");
    assert(len == 0);  // All data consumed

    printf("  [PASS] test_integration_batch_commands\n");
}

static void test_integration_partial_command() {
    // Simulate a command split across two reads
    Store<> store;
    char buf[256];

    // First read: partial SET command
    const char* seg1 = "SET key1 val";
    std::size_t len = strlen(seg1);
    memcpy(buf, seg1, len);

    std::string r1 = process_buffer(buf, len, store);
    assert(r1.empty());    // No complete command yet
    assert(len == 12);     // All 12 bytes retained

    // Second read: rest of the command
    const char* seg2 = "ue1\r\n";
    std::size_t seg2_len = strlen(seg2);
    memcpy(buf + len, seg2, seg2_len);
    len += seg2_len;

    std::string r2 = process_buffer(buf, len, store);
    assert(r2 == "+OK\r\n");
    assert(len == 0);      // All data consumed

    // Verify the value was stored correctly via the Store API
    auto val = store.get("key1");
    assert(val.has_value());
    assert(*val == "value1");

    printf("  [PASS] test_integration_partial_command\n");
}

static void test_integration_command_then_partial() {
    // One complete command followed by a partial command
    Store<> store;
    char buf[256];
    const char* input = "PING\r\nSET key";
    std::size_t len = strlen(input);
    memcpy(buf, input, len);

    std::string response = process_buffer(buf, len, store);
    assert(response == "+PONG\r\n");    // Only PING was complete
    assert(len == 7);                    // "SET key" (7 bytes) retained

    // Verify the retained data
    assert(memcmp(buf, "SET key", 7) == 0);

    printf("  [PASS] test_integration_command_then_partial\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Main — Run all tests
// ═════════════════════════════════════════════════════════════════════════════

int main() {
    printf("=== Parser Tests ===\n");
    test_parse_single_command();
    test_parse_command_with_args();
    test_parse_incomplete_command();
    test_parse_empty_buffer();
    test_parse_empty_line();
    test_parse_bare_newline();
    test_parse_extra_spaces();
    test_parse_multiple_commands_in_buffer();
    test_parse_too_many_args();
    test_parse_resumable();
    test_parse_zero_copy();

    printf("\n=== Response Formatter Tests ===\n");
    test_resp_simple_string();
    test_resp_error();
    test_resp_bulk_string();
    test_resp_bulk_string_empty();
    test_resp_nil();
    test_resp_integer();
    test_resp_append_multiple();

    printf("\n=== Command Dispatch Tests ===\n");
    test_cmd_ping();
    test_cmd_ping_with_message();
    test_cmd_ping_case_insensitive();
    test_cmd_set_get();
    test_cmd_get_nonexistent();
    test_cmd_del();
    test_cmd_unknown();
    test_cmd_wrong_args_get();
    test_cmd_wrong_args_set();

    printf("\n=== Integration Tests ===\n");
    test_integration_batch_commands();
    test_integration_partial_command();
    test_integration_command_then_partial();

    printf("\n========================================\n");
    printf("ALL TESTS PASSED (31 tests)\n");
    printf("========================================\n");

    return 0;
}
