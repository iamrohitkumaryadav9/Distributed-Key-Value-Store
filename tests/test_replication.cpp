// ─────────────────────────────────────────────────────────────────────────────
// test_replication.cpp — Integration tests for single-leader replication
//
// Tests cover:
//   1. ReplicationManager start/stop lifecycle
//   2. Full sync: leader WAL entries replicate to new replica
//   3. Live replication: writes after replica connects arrive at replica
//   4. Read-only guard: replicas reject SET/DEL
//   5. Replica disconnect + reconnect: new replica gets full state
//   6. Multiple replicas: all receive broadcasts
// ─────────────────────────────────────────────────────────────────────────────

#include "protocol.h"
#include "replication.h"
#include "store.h"
#include "wal.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>      // memset
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Create a temporary directory for test files
// ─────────────────────────────────────────────────────────────────────────────

static std::string make_temp_dir() {
    char tmpl[] = "/tmp/kvstore_test_repl_XXXXXX";
    char* dir = mkdtemp(tmpl);
    assert(dir != nullptr);
    return std::string(dir);
}

static void cleanup_dir(const std::string& dir) {
    std::filesystem::remove_all(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Wait for a condition with timeout
// ─────────────────────────────────────────────────────────────────────────────

template<typename Pred>
static bool wait_for(Pred pred, int timeout_ms = 3000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;  // Timed out
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: ReplicationManager Lifecycle
// ═════════════════════════════════════════════════════════════════════════════

static void test_repl_manager_lifecycle() {
    std::string dir = make_temp_dir();
    std::string wal_path = dir + "/test.wal";

    // Create a WAL file
    {
        WAL wal(wal_path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
    }

    // Start and stop the replication manager
    {
        ReplicationManager mgr(16380);
        auto err = mgr.start(wal_path);
        assert(err.empty());
        assert(mgr.replica_count() == 0);
        mgr.stop();
    }

    cleanup_dir(dir);
    printf("  [PASS] test_repl_manager_lifecycle\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: Full Sync — Existing WAL data replicates to new replica
// ═════════════════════════════════════════════════════════════════════════════

static void test_full_sync() {
    std::string dir = make_temp_dir();
    std::string wal_path = dir + "/leader.wal";

    // Write data to leader's WAL
    {
        WAL wal(wal_path, SyncPolicy::kNone);
        auto err = wal.open();
        assert(err.empty());
        assert(wal.append_set("key1", "value1"));
        assert(wal.append_set("key2", "value2"));
        assert(wal.append_set("key3", "value3"));
        assert(wal.append_del("key2"));
    }

    // Start replication manager
    ReplicationManager mgr(16381);
    auto err = mgr.start(wal_path);
    assert(err.empty());

    // Create a replica store and connect
    Store<> replica_store;
    ReplicaClient client("127.0.0.1", 16381, replica_store);
    err = client.start();
    assert(err.empty());

    // Wait for full sync to complete
    // The WAL has 4 entries (SET key1, SET key2, SET key3, DEL key2)
    bool synced = wait_for([&client]() {
        return client.entries_received() >= 4;
    });
    assert(synced);

    // Verify replica state matches expected state after replay
    auto v1 = replica_store.get("key1");
    assert(v1.has_value() && *v1 == "value1");

    auto v2 = replica_store.get("key2");
    assert(!v2.has_value());  // Deleted

    auto v3 = replica_store.get("key3");
    assert(v3.has_value() && *v3 == "value3");

    // Clean up
    client.stop();
    mgr.stop();

    cleanup_dir(dir);
    printf("  [PASS] test_full_sync\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: Live Replication — Writes after replica connects
// ═════════════════════════════════════════════════════════════════════════════

static void test_live_replication() {
    std::string dir = make_temp_dir();
    std::string wal_path = dir + "/leader.wal";

    // Create empty WAL
    WAL wal(wal_path, SyncPolicy::kNone);
    auto err = wal.open();
    assert(err.empty());

    // Start replication manager
    ReplicationManager mgr(16382);
    err = mgr.start(wal_path);
    assert(err.empty());

    // Connect replica
    Store<> replica_store;
    ReplicaClient client("127.0.0.1", 16382, replica_store);
    err = client.start();
    assert(err.empty());

    // Wait for replica to connect
    bool connected = wait_for([&mgr]() {
        return mgr.replica_count() > 0;
    });
    assert(connected);

    // Write data to leader's WAL and broadcast
    {
        std::vector<uint8_t> bytes;
        assert(wal.append_set("live_key1", "live_val1", &bytes));
        mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
    }
    {
        std::vector<uint8_t> bytes;
        assert(wal.append_set("live_key2", "live_val2", &bytes));
        mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
    }
    {
        std::vector<uint8_t> bytes;
        assert(wal.append_del("live_key1", &bytes));
        mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
    }

    // Wait for replica to receive all 3 entries
    bool received = wait_for([&client]() {
        return client.entries_received() >= 3;
    });
    assert(received);

    // Verify replica state
    auto v1 = replica_store.get("live_key1");
    assert(!v1.has_value());  // Deleted

    auto v2 = replica_store.get("live_key2");
    assert(v2.has_value() && *v2 == "live_val2");

    client.stop();
    mgr.stop();
    cleanup_dir(dir);
    printf("  [PASS] test_live_replication\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: Read-Only Guard — Replicas reject writes
// ═════════════════════════════════════════════════════════════════════════════

static void test_read_only_guard() {
    Store<> store;
    store.set("existing_key", "existing_value");

    // Simulate a SET command
    ParseResult set_cmd;
    set_cmd.status = ParseStatus::kComplete;
    set_cmd.num_args = 3;
    set_cmd.args[0] = "SET";
    set_cmd.args[1] = "key";
    set_cmd.args[2] = "value";
    set_cmd.bytes_consumed = 0;

    std::string out;
    dispatch_command(set_cmd, out, store, nullptr, nullptr, true);
    assert(out.find("READONLY") != std::string::npos);

    // Simulate a DEL command
    out.clear();
    ParseResult del_cmd;
    del_cmd.status = ParseStatus::kComplete;
    del_cmd.num_args = 2;
    del_cmd.args[0] = "DEL";
    del_cmd.args[1] = "existing_key";
    del_cmd.bytes_consumed = 0;

    dispatch_command(del_cmd, out, store, nullptr, nullptr, true);
    assert(out.find("READONLY") != std::string::npos);

    // Verify store was NOT modified
    auto v = store.get("existing_key");
    assert(v.has_value() && *v == "existing_value");

    // GET should still work on read-only replica
    out.clear();
    ParseResult get_cmd;
    get_cmd.status = ParseStatus::kComplete;
    get_cmd.num_args = 2;
    get_cmd.args[0] = "GET";
    get_cmd.args[1] = "existing_key";
    get_cmd.bytes_consumed = 0;

    dispatch_command(get_cmd, out, store, nullptr, nullptr, true);
    // Should return the value, not READONLY error
    assert(out.find("existing_value") != std::string::npos);
    assert(out.find("READONLY") == std::string::npos);

    // PING should work on read-only replica
    out.clear();
    ParseResult ping_cmd;
    ping_cmd.status = ParseStatus::kComplete;
    ping_cmd.num_args = 1;
    ping_cmd.args[0] = "PING";
    ping_cmd.bytes_consumed = 0;

    dispatch_command(ping_cmd, out, store, nullptr, nullptr, true);
    assert(out.find("PONG") != std::string::npos);

    printf("  [PASS] test_read_only_guard\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: Multiple Replicas — All receive broadcasts
// ═════════════════════════════════════════════════════════════════════════════

static void test_multiple_replicas() {
    std::string dir = make_temp_dir();
    std::string wal_path = dir + "/leader.wal";

    // Create empty WAL
    WAL wal(wal_path, SyncPolicy::kNone);
    auto err = wal.open();
    assert(err.empty());

    // Start replication manager
    ReplicationManager mgr(16383);
    err = mgr.start(wal_path);
    assert(err.empty());

    // Connect 3 replicas
    Store<> store1, store2, store3;
    ReplicaClient client1("127.0.0.1", 16383, store1);
    ReplicaClient client2("127.0.0.1", 16383, store2);
    ReplicaClient client3("127.0.0.1", 16383, store3);

    assert(client1.start().empty());
    assert(client2.start().empty());
    assert(client3.start().empty());

    // Wait for all replicas to connect
    bool all_connected = wait_for([&mgr]() {
        return mgr.replica_count() >= 3;
    });
    assert(all_connected);

    // Broadcast a write
    {
        std::vector<uint8_t> bytes;
        assert(wal.append_set("shared_key", "shared_value", &bytes));
        mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
    }

    // Wait for all replicas to receive it
    bool all_received = wait_for([&]() {
        return client1.entries_received() >= 1 &&
               client2.entries_received() >= 1 &&
               client3.entries_received() >= 1;
    });
    assert(all_received);

    // Verify all replicas have the data
    auto v1 = store1.get("shared_key");
    auto v2 = store2.get("shared_key");
    auto v3 = store3.get("shared_key");

    assert(v1.has_value() && *v1 == "shared_value");
    assert(v2.has_value() && *v2 == "shared_value");
    assert(v3.has_value() && *v3 == "shared_value");

    client1.stop();
    client2.stop();
    client3.stop();
    mgr.stop();
    cleanup_dir(dir);
    printf("  [PASS] test_multiple_replicas\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Test: Replica Reconnect — New replica gets full state
// ═════════════════════════════════════════════════════════════════════════════

static void test_replica_reconnect() {
    std::string dir = make_temp_dir();
    std::string wal_path = dir + "/leader.wal";

    // Create WAL with data
    WAL wal(wal_path, SyncPolicy::kNone);
    auto err = wal.open();
    assert(err.empty());

    // Start replication manager
    ReplicationManager mgr(16384);
    err = mgr.start(wal_path);
    assert(err.empty());

    // First replica connects, gets data, then disconnects
    {
        Store<> store;
        ReplicaClient client("127.0.0.1", 16384, store);
        assert(client.start().empty());

        // Wait for connection
        bool connected = wait_for([&mgr]() {
            return mgr.replica_count() > 0;
        });
        assert(connected);

        // Broadcast some data
        {
            std::vector<uint8_t> bytes;
            assert(wal.append_set("persist_key", "persist_value", &bytes));
            mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
        }

        bool received = wait_for([&client]() {
            return client.entries_received() >= 1;
        });
        assert(received);

        auto v = store.get("persist_key");
        assert(v.has_value() && *v == "persist_value");

        // Disconnect
        client.stop();
    }

    // Wait for leader to detect disconnect
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Broadcast more data while no replica is connected
    {
        std::vector<uint8_t> bytes;
        assert(wal.append_set("offline_key", "offline_value", &bytes));
        mgr.broadcast(bytes.data(), static_cast<uint32_t>(bytes.size()));
    }

    // New replica connects — should get ALL data via full sync
    {
        Store<> store;
        ReplicaClient client("127.0.0.1", 16384, store);
        assert(client.start().empty());

        // Should receive 2 entries (persist_key + offline_key) from full sync
        bool received = wait_for([&client]() {
            return client.entries_received() >= 2;
        });
        assert(received);

        auto v1 = store.get("persist_key");
        assert(v1.has_value() && *v1 == "persist_value");

        auto v2 = store.get("offline_key");
        assert(v2.has_value() && *v2 == "offline_value");

        client.stop();
    }

    mgr.stop();
    cleanup_dir(dir);
    printf("  [PASS] test_replica_reconnect\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

int main() {
    printf("=== ReplicationManager Lifecycle ===\n");
    test_repl_manager_lifecycle();

    printf("\n=== Full Sync ===\n");
    test_full_sync();

    printf("\n=== Live Replication ===\n");
    test_live_replication();

    printf("\n=== Read-Only Guard ===\n");
    test_read_only_guard();

    printf("\n=== Multiple Replicas ===\n");
    test_multiple_replicas();

    printf("\n=== Replica Reconnect ===\n");
    test_replica_reconnect();

    printf("\n========================================\n");
    printf("ALL REPLICATION TESTS PASSED (6 tests)\n");
    printf("========================================\n");

    return 0;
}
