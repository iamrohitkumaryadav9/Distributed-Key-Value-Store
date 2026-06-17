// ─────────────────────────────────────────────────────────────────────────────
// test_wal.cpp — Unit tests for the Write-Ahead Log
//
// Tests cover:
//   1. Basic write + replay (SET, DEL)
//   2. CRC32 correctness (known test vectors)
//   3. Corruption detection (flip a byte, verify CRC mismatch stops replay)
//   4. Partial write recovery (truncate mid-entry, verify clean replay)
//   5. Empty WAL replay
//   6. WAL truncation
//   7. Large entries (exceeding stack buffer)
//   8. Multiple replay cycles
// ─────────────────────────────────────────────────────────────────────────────

#include "wal.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>    // mkdtemp
#include <cstring>    // strlen, memcpy
#include <fcntl.h>    // open
#include <string>
#include <sys/stat.h> // stat
#include <unistd.h>   // pwrite, pread, unlink, rmdir, close
#include <vector>
#include <filesystem>

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Create a temporary directory for test WAL files
// ─────────────────────────────────────────────────────────────────────────────

static std::string make_temp_dir() {
    // Create temp dir inside the build directory to stay in workspace
    char tmpl[] = "/tmp/kvstore_test_wal_XXXXXX";
    char* dir = mkdtemp(tmpl);
    assert(dir != nullptr);
    return std::string(dir);
}

// Helper: cleanup
static void cleanup_dir(const std::string& dir) {
    std::filesystem::remove_all(dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// CRC32 Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_crc32_empty() {
    // CRC32 of empty input is 0x00000000
    uint32_t crc = WAL::compute_crc32(nullptr, 0);
    assert(crc == 0x00000000);

    printf("  [PASS] test_crc32_empty\n");
}

static void test_crc32_known_vectors() {
    // CRC32 of "123456789" is 0xCBF43926 (standard test vector)
    const char* input = "123456789";
    uint32_t crc = WAL::compute_crc32(
        reinterpret_cast<const uint8_t*>(input), 9);
    assert(crc == 0xCBF43926);

    printf("  [PASS] test_crc32_known_vectors\n");
}

static void test_crc32_deterministic() {
    // Same input must produce the same CRC
    const char* input = "hello world";
    uint32_t crc1 = WAL::compute_crc32(
        reinterpret_cast<const uint8_t*>(input), strlen(input));
    uint32_t crc2 = WAL::compute_crc32(
        reinterpret_cast<const uint8_t*>(input), strlen(input));
    assert(crc1 == crc2);

    // Different input must produce different CRC
    const char* input2 = "hello worlD";
    uint32_t crc3 = WAL::compute_crc32(
        reinterpret_cast<const uint8_t*>(input2), strlen(input2));
    assert(crc1 != crc3);

    printf("  [PASS] test_crc32_deterministic\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Basic Write + Replay Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_write_and_replay_set() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Write entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        assert(wal.append_set("key1", "value1"));
        assert(wal.append_set("key2", "value2"));
        assert(wal.append_set("key3", "value3"));
    }
    // WAL destructor closes the file

    // Replay entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(count == 3);
        assert(entries[0].op == WAL::Entry::OpType::kSet);
        assert(entries[0].key == "key1");
        assert(entries[0].value == "value1");
        assert(entries[1].key == "key2");
        assert(entries[1].value == "value2");
        assert(entries[2].key == "key3");
        assert(entries[2].value == "value3");

        // Timestamps should be non-zero and increasing
        assert(entries[0].timestamp_ns > 0);
        assert(entries[1].timestamp_ns >= entries[0].timestamp_ns);
        assert(entries[2].timestamp_ns >= entries[1].timestamp_ns);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_write_and_replay_set\n");
}

static void test_write_and_replay_del() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");
        wal.append_del("key1");
    }

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(entries.size() == 2);
        assert(entries[0].op == WAL::Entry::OpType::kSet);
        assert(entries[0].key == "key1");
        assert(entries[1].op == WAL::Entry::OpType::kDel);
        assert(entries[1].key == "key1");
        assert(entries[1].value.empty());  // DEL has no value
    }

    cleanup_dir(dir);
    printf("  [PASS] test_write_and_replay_del\n");
}

static void test_empty_wal_replay() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::size_t count = wal.replay([](const WAL::Entry&) {
            assert(false);  // Should never be called
        });
        assert(count == 0);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_empty_wal_replay\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Corruption Detection Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_corruption_in_second_entry() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Write 3 entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");  // Entry 1
        wal.append_set("key2", "value2");  // Entry 2
        wal.append_set("key3", "value3");  // Entry 3
    }

    // Calculate offset of entry 2.
    // Entry 1: entry_len_field(4) + timestamp(8) + op(1) + key_len(2)
    //        + "key1"(4) + val_len(2) + "value1"(6) + crc32(4)
    //        = 4 + 8 + 1 + 2 + 4 + 2 + 6 + 4 = 31 bytes
    // Entry 2 starts at offset 31, payload starts at offset 35.
    // Corrupt a byte inside entry 2's payload (e.g., offset 40).

    {
        int fd = ::open(path.c_str(), O_RDWR);
        assert(fd != -1);
        uint8_t garbage = 0xFF;
        ssize_t w = pwrite(fd, &garbage, 1, 40);
        assert(w == 1);
        close(fd);
    }

    // Replay — should get entry 1, then CRC mismatch on entry 2
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        // Entry 1 should replay fine, entry 2 and 3 should be lost
        assert(count == 1);
        assert(entries[0].key == "key1");
        assert(entries[0].value == "value1");
    }

    cleanup_dir(dir);
    printf("  [PASS] test_corruption_in_second_entry\n");
}

static void test_corruption_in_first_entry() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");
        wal.append_set("key2", "value2");
    }

    // Corrupt entry 1's payload (offset 10, inside the timestamp)
    {
        int fd = ::open(path.c_str(), O_RDWR);
        assert(fd != -1);
        uint8_t garbage = 0xFF;
        ssize_t w = pwrite(fd, &garbage, 1, 10);
        assert(w == 1);
        close(fd);
    }

    // Replay should get 0 entries (first entry is corrupted)
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::size_t count = wal.replay([](const WAL::Entry&) {});
        assert(count == 0);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_corruption_in_first_entry\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Partial Write Recovery Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_partial_write_truncated_entry() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Write 2 complete entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");
        wal.append_set("key2", "value2");
    }

    // Get file size, then truncate to simulate a crash mid-write of entry 3
    struct stat st;
    memset(&st, 0, sizeof(st));
    ::stat(path.c_str(), &st);
    off_t full_size = st.st_size;

    // Append a partial third entry (write 10 bytes of garbage)
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        assert(fd != -1);
        uint8_t garbage[10] = {0x0A, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
        ssize_t w = ::write(fd, garbage, sizeof(garbage));
        assert(w == 10);
        close(fd);
    }

    // Replay — should get 2 entries, partial garbage is discarded
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(count == 2);
        assert(entries[0].key == "key1");
        assert(entries[1].key == "key2");

        // After replay, write position should be at the end of entry 2
        assert(wal.write_position() == full_size);
    }

    // Verify file was truncated (garbage removed)
    memset(&st, 0, sizeof(st));
    ::stat(path.c_str(), &st);
    assert(st.st_size == full_size);

    cleanup_dir(dir);
    printf("  [PASS] test_partial_write_truncated_entry\n");
}

static void test_partial_entry_len_field() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Write 1 complete entry
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");
    }

    // Append only 2 bytes (incomplete entry_len field)
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        assert(fd != -1);
        uint8_t garbage[2] = {0xFF, 0xFF};
        ssize_t w = ::write(fd, garbage, 2);
        assert(w == 2);
        close(fd);
    }

    // Replay — should get 1 entry, partial entry_len is discarded
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::size_t count = wal.replay([](const WAL::Entry&) {});
        assert(count == 1);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_partial_entry_len_field\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// WAL Truncation
// ═════════════════════════════════════════════════════════════════════════════

static void test_wal_truncate() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        // Write entries
        wal.append_set("key1", "value1");
        wal.append_set("key2", "value2");
        assert(wal.write_position() > 0);

        // Truncate
        assert(wal.truncate());
        assert(wal.write_position() == 0);

        // Write new entries after truncate
        wal.append_set("key3", "value3");

        // Replay should only see key3
        std::vector<WAL::Entry> entries;
        // Need to reopen for replay from beginning
    }

    // Reopen and replay
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(count == 1);
        assert(entries[0].key == "key3");
    }

    cleanup_dir(dir);
    printf("  [PASS] test_wal_truncate\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Large Entry Test (exceeds stack buffer)
// ═════════════════════════════════════════════════════════════════════════════

static void test_large_entry() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Create a value larger than 4KB to force heap allocation in append_entry
    std::string large_value(8192, 'X');

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        assert(wal.append_set("bigkey", large_value));
    }

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(count == 1);
        assert(entries[0].key == "bigkey");
        assert(entries[0].value == large_value);
        assert(entries[0].value.size() == 8192);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_large_entry\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Append After Replay (continued operation)
// ═════════════════════════════════════════════════════════════════════════════

static void test_append_after_replay() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    // Write initial entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        wal.append_set("key1", "value1");
        wal.append_set("key2", "value2");
    }

    // Reopen, replay, then append more
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::size_t count = wal.replay([](const WAL::Entry&) {});
        assert(count == 2);

        // Append new entries after replay
        wal.append_set("key3", "value3");
        wal.append_del("key1");
    }

    // Final replay should see all 4 entries
    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        std::vector<WAL::Entry> entries;
        std::size_t count = wal.replay([&entries](const WAL::Entry& e) {
            entries.push_back({e.op, e.key, e.value, e.timestamp_ns});
        });

        assert(count == 4);
        assert(entries[0].key == "key1");
        assert(entries[0].op == WAL::Entry::OpType::kSet);
        assert(entries[1].key == "key2");
        assert(entries[2].key == "key3");
        assert(entries[3].key == "key1");
        assert(entries[3].op == WAL::Entry::OpType::kDel);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_append_after_replay\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Many Entries (stress test)
// ═════════════════════════════════════════════════════════════════════════════

static void test_many_entries() {
    std::string dir = make_temp_dir();
    std::string path = dir + "/test.wal";

    constexpr int N = 10000;

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        for (int i = 0; i < N; ++i) {
            std::string key = "key:" + std::to_string(i);
            std::string val = "val:" + std::to_string(i);
            assert(wal.append_set(key, val));
        }
    }

    {
        WAL wal(path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());

        int idx = 0;
        std::size_t count = wal.replay([&idx](const WAL::Entry& e) {
            std::string expected_key = "key:" + std::to_string(idx);
            std::string expected_val = "val:" + std::to_string(idx);
            assert(e.key == expected_key);
            assert(e.value == expected_val);
            ++idx;
        });

        assert(count == N);
        assert(idx == N);
    }

    cleanup_dir(dir);
    printf("  [PASS] test_many_entries\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

int main() {
    printf("=== CRC32 Tests ===\n");
    test_crc32_empty();
    test_crc32_known_vectors();
    test_crc32_deterministic();

    printf("\n=== Basic Write + Replay Tests ===\n");
    test_write_and_replay_set();
    test_write_and_replay_del();
    test_empty_wal_replay();

    printf("\n=== Corruption Detection Tests ===\n");
    test_corruption_in_second_entry();
    test_corruption_in_first_entry();

    printf("\n=== Partial Write Recovery Tests ===\n");
    test_partial_write_truncated_entry();
    test_partial_entry_len_field();

    printf("\n=== WAL Truncation ===\n");
    test_wal_truncate();

    printf("\n=== Large Entry ===\n");
    test_large_entry();

    printf("\n=== Append After Replay ===\n");
    test_append_after_replay();

    printf("\n=== Stress Test ===\n");
    test_many_entries();

    printf("\n========================================\n");
    printf("ALL WAL TESTS PASSED (13 tests)\n");
    printf("========================================\n");

    return 0;
}
