// ─────────────────────────────────────────────────────────────────────────────
// test_store.cpp — Unit tests for the sharded concurrent hash map
//
// Tests cover:
//   1. Basic operations: get, set, del on Store<16>
//   2. Overwrite and delete semantics
//   3. TTL: set_with_ttl, expiry, lazy deletion
//   4. Shard distribution: verify keys hash to different shards
//   5. Thread safety: concurrent reads and writes from multiple threads
//   6. Heterogeneous lookup: string_view keys without allocation
// ─────────────────────────────────────────────────────────────────────────────

#include "store.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

// ═════════════════════════════════════════════════════════════════════════════
// Basic Operations
// ═════════════════════════════════════════════════════════════════════════════

static void test_get_nonexistent() {
    Store<> store;
    auto val = store.get("nosuchkey");
    assert(!val.has_value());

    printf("  [PASS] test_get_nonexistent\n");
}

static void test_set_and_get() {
    Store<> store;
    store.set("key1", "value1");

    auto val = store.get("key1");
    assert(val.has_value());
    assert(*val == "value1");

    printf("  [PASS] test_set_and_get\n");
}

static void test_set_overwrite() {
    Store<> store;
    store.set("key1", "value1");
    store.set("key1", "value2");

    auto val = store.get("key1");
    assert(val.has_value());
    assert(*val == "value2");

    printf("  [PASS] test_set_overwrite\n");
}

static void test_del_existing() {
    Store<> store;
    store.set("key1", "value1");

    bool deleted = store.del("key1");
    assert(deleted == true);

    auto val = store.get("key1");
    assert(!val.has_value());

    printf("  [PASS] test_del_existing\n");
}

static void test_del_nonexistent() {
    Store<> store;
    bool deleted = store.del("nosuchkey");
    assert(deleted == false);

    printf("  [PASS] test_del_nonexistent\n");
}

static void test_multiple_keys() {
    Store<> store;
    // Insert 100 keys
    for (int i = 0; i < 100; ++i) {
        std::string key = "key:" + std::to_string(i);
        std::string val = "val:" + std::to_string(i);
        store.set(key, val);
    }

    // Verify all 100 keys
    for (int i = 0; i < 100; ++i) {
        std::string key = "key:" + std::to_string(i);
        std::string expected = "val:" + std::to_string(i);
        auto val = store.get(key);
        assert(val.has_value());
        assert(*val == expected);
    }

    printf("  [PASS] test_multiple_keys\n");
}

static void test_heterogeneous_lookup() {
    // Verify string_view lookups work correctly (transparent hash)
    Store<> store;
    store.set("key1", "value1");

    std::string_view key_view = "key1";
    auto val = store.get(key_view);
    assert(val.has_value());
    assert(*val == "value1");

    printf("  [PASS] test_heterogeneous_lookup\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// TTL Tests
// ═════════════════════════════════════════════════════════════════════════════

static void test_ttl_not_expired() {
    Store<> store;
    // Set a key with 10-second TTL — should still be valid
    store.set_with_ttl("key1", "value1", std::chrono::milliseconds(10000));

    auto val = store.get("key1");
    assert(val.has_value());
    assert(*val == "value1");

    printf("  [PASS] test_ttl_not_expired\n");
}

static void test_ttl_expired() {
    Store<> store;
    // Set a key with 1ms TTL
    store.set_with_ttl("key1", "value1", std::chrono::milliseconds(1));

    // Wait for expiry
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Key should be expired now
    auto val = store.get("key1");
    assert(!val.has_value());

    printf("  [PASS] test_ttl_expired\n");
}

static void test_ttl_overwrite_removes_ttl() {
    Store<> store;
    // Set with TTL
    store.set_with_ttl("key1", "value1", std::chrono::milliseconds(1));

    // Overwrite without TTL — should live forever now
    store.set("key1", "value2");

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto val = store.get("key1");
    assert(val.has_value());
    assert(*val == "value2");

    printf("  [PASS] test_ttl_overwrite_removes_ttl\n");
}

static void test_ttl_lazy_deletion() {
    Store<> store;
    // Set with 1ms TTL
    store.set_with_ttl("key1", "value1", std::chrono::milliseconds(1));

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // First GET triggers lazy deletion
    auto val1 = store.get("key1");
    assert(!val1.has_value());

    // Second GET should also return nullopt (key was deleted)
    auto val2 = store.get("key1");
    assert(!val2.has_value());

    printf("  [PASS] test_ttl_lazy_deletion\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Shard Distribution
// ═════════════════════════════════════════════════════════════════════════════

static void test_shard_distribution() {
    // Verify that keys distribute across multiple shards.
    // With 1000 random-ish keys and 16 shards, each shard should get
    // at least some keys (probabilistic, but extremely unlikely to fail).
    //
    // We can't directly inspect shard assignment from outside the Store,
    // but we can verify the shard_index logic matches our expectation.
    constexpr std::size_t NUM_SHARDS = 16;
    bool shard_hit[NUM_SHARDS] = {};

    for (int i = 0; i < 1000; ++i) {
        std::string key = "test_key_" + std::to_string(i);
        std::size_t idx = std::hash<std::string_view>{}(key) & (NUM_SHARDS - 1);
        assert(idx < NUM_SHARDS);
        shard_hit[idx] = true;
    }

    // All 16 shards should have been hit at least once
    for (std::size_t i = 0; i < NUM_SHARDS; ++i) {
        assert(shard_hit[i]);
    }

    printf("  [PASS] test_shard_distribution\n");
}

static void test_single_shard_store() {
    // Verify Store<1> works (single shard = all keys in one shard)
    Store<1> store;
    store.set("a", "1");
    store.set("b", "2");

    assert(store.get("a").has_value());
    assert(*store.get("a") == "1");
    assert(store.get("b").has_value());
    assert(*store.get("b") == "2");
    assert(!store.get("c").has_value());

    printf("  [PASS] test_single_shard_store\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Thread Safety
// ═════════════════════════════════════════════════════════════════════════════

static void test_concurrent_reads() {
    Store<> store;

    // Pre-populate
    for (int i = 0; i < 1000; ++i) {
        store.set("key:" + std::to_string(i), "val:" + std::to_string(i));
    }

    // Launch 4 reader threads, each reading all keys
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&store, &errors]() {
            for (int i = 0; i < 1000; ++i) {
                std::string key = "key:" + std::to_string(i);
                std::string expected = "val:" + std::to_string(i);
                auto val = store.get(key);
                if (!val.has_value() || *val != expected) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) t.join();
    assert(errors.load() == 0);

    printf("  [PASS] test_concurrent_reads\n");
}

static void test_concurrent_writes() {
    Store<> store;

    // 4 threads each write 1000 unique keys
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&store, t]() {
            for (int i = 0; i < 1000; ++i) {
                std::string key = "t" + std::to_string(t) + ":key:" + std::to_string(i);
                std::string val = "val:" + std::to_string(i);
                store.set(key, val);
            }
        });
    }

    for (auto& t : threads) t.join();

    // Verify all 4000 keys exist
    for (int t = 0; t < 4; ++t) {
        for (int i = 0; i < 1000; ++i) {
            std::string key = "t" + std::to_string(t) + ":key:" + std::to_string(i);
            auto val = store.get(key);
            assert(val.has_value());
        }
    }

    printf("  [PASS] test_concurrent_writes\n");
}

static void test_concurrent_read_write() {
    Store<> store;

    // Pre-populate with initial values
    for (int i = 0; i < 500; ++i) {
        store.set("key:" + std::to_string(i), "initial:" + std::to_string(i));
    }

    std::atomic<int> read_errors{0};
    std::vector<std::thread> threads;

    // 2 writer threads updating existing keys
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&store, t]() {
            for (int round = 0; round < 100; ++round) {
                for (int i = 0; i < 500; ++i) {
                    std::string key = "key:" + std::to_string(i);
                    std::string val = "t" + std::to_string(t) +
                                     ":r" + std::to_string(round) +
                                     ":v" + std::to_string(i);
                    store.set(key, val);
                }
            }
        });
    }

    // 2 reader threads reading continuously
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&store, &read_errors]() {
            for (int round = 0; round < 100; ++round) {
                for (int i = 0; i < 500; ++i) {
                    std::string key = "key:" + std::to_string(i);
                    auto val = store.get(key);
                    // Value must exist (we never delete), but the exact value
                    // depends on the writer's progress — we just check existence.
                    if (!val.has_value()) {
                        read_errors.fetch_add(1);
                    }
                }
            }
        });
    }

    for (auto& t : threads) t.join();
    assert(read_errors.load() == 0);

    printf("  [PASS] test_concurrent_read_write\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Static Assertions (verified at compile time)
// ═════════════════════════════════════════════════════════════════════════════

static void test_compile_time_checks() {
    // These are compile-time checks — if we reach this point, they passed.
    // Store<16> is the default, NUM_SHARDS = 16
    static_assert(Store<16>::NUM_SHARDS == 16);
    static_assert(Store<1>::NUM_SHARDS == 1);

    // Power-of-2 constraint is enforced by static_assert in Store
    // Uncommenting this would cause a compile error:
    // Store<3> bad_store;  // "NUM_SHARDS must be a power of 2"

    printf("  [PASS] test_compile_time_checks\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

int main() {
    printf("=== Basic Operations ===\n");
    test_get_nonexistent();
    test_set_and_get();
    test_set_overwrite();
    test_del_existing();
    test_del_nonexistent();
    test_multiple_keys();
    test_heterogeneous_lookup();

    printf("\n=== TTL Tests ===\n");
    test_ttl_not_expired();
    test_ttl_expired();
    test_ttl_overwrite_removes_ttl();
    test_ttl_lazy_deletion();

    printf("\n=== Shard Distribution ===\n");
    test_shard_distribution();
    test_single_shard_store();

    printf("\n=== Thread Safety ===\n");
    test_concurrent_reads();
    test_concurrent_writes();
    test_concurrent_read_write();

    printf("\n=== Compile-Time Checks ===\n");
    test_compile_time_checks();

    printf("\n========================================\n");
    printf("ALL STORE TESTS PASSED (18 tests)\n");
    printf("========================================\n");

    return 0;
}
