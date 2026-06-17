#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// replication.h — Single-leader replication for the distributed KV store
//
// Architecture:
//   Leader (accepts writes):
//     - Runs a client port (6379) for RESP commands
//     - Runs a replication port (6380) for replica connections
//     - On write: WAL → store → broadcast to all replicas
//     - Full sync: sends entire WAL file to newly connected replicas
//
//   Replica (read-only):
//     - Connects to leader's replication port
//     - Receives WAL entries (full sync + live stream)
//     - Applies entries to local store
//     - Serves read-only queries (GET, PING) from clients
//     - Rejects writes (SET, DEL) with READONLY error
//
// Replication protocol:
//   The leader sends raw WAL binary entries over TCP. The format is
//   identical to the on-disk WAL format (entry_len + payload + crc32).
//   This reuses all WAL parsing and CRC32 validation logic — zero new
//   serialization code needed.
//
// Consistency model:
//   Eventual consistency — replicas lag behind the leader by network
//   latency. Writes are acknowledged by the leader without waiting
//   for replica confirmation (asynchronous replication).
//
// Failure handling:
//   - If a replica disconnects, the leader removes it from the list.
//   - If a replica reconnects, it gets a fresh full sync.
//   - If the leader crashes, replicas stop receiving updates and serve
//     stale data. Manual leader promotion is required (no automatic
//     failover in this layer).
// ─────────────────────────────────────────────────────────────────────────────

#include "store.h"      // Store<> for ReplicaClient
#include "wal.h"        // WAL::compute_crc32, wal_format constants

#include <atomic>       // std::atomic for thread control
#include <cstddef>      // std::size_t
#include <cstdint>      // uint8_t, uint16_t, uint32_t
#include <mutex>        // std::mutex for replica list
#include <string>       // std::string
#include <thread>       // std::thread for background loops
#include <vector>       // std::vector for replica fds and buffers

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationManager — Leader-side replica management
// ─────────────────────────────────────────────────────────────────────────────
// Manages a TCP listener for replica connections and broadcasts WAL
// entries to all connected replicas.
//
// Thread safety:
//   - accept_loop() runs in a background thread
//   - broadcast() is called from the main thread (inside dispatch_command)
//   - replicas_mutex_ protects the replica fd list
//   - Full sync holds the lock to prevent missed entries

class ReplicationManager {
public:
    explicit ReplicationManager(uint16_t port);
    ~ReplicationManager();

    // Non-copyable
    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    // Start listening for replica connections.
    // wal_path: path to the WAL file (read during full sync).
    // Returns empty string on success, error message on failure.
    [[nodiscard]] std::string start(const std::string& wal_path);

    // Broadcast a serialized WAL entry (entry_len + payload + crc32)
    // to all connected replicas. If a write to a replica fails, that
    // replica is removed from the list (graceful disconnect).
    //
    // Called from the main thread after a successful WAL write.
    void broadcast(const uint8_t* data, uint32_t len);

    // Stop the listener and disconnect all replicas.
    void stop();

    // Number of connected replicas (for monitoring / logging).
    std::size_t replica_count() const;

private:
    uint16_t port_;
    int listen_fd_ = -1;
    std::string wal_path_;

    std::thread accept_thread_;
    std::atomic<bool> running_{false};

    mutable std::mutex replicas_mutex_;
    std::vector<int> replica_fds_;

    // Background accept loop — runs in accept_thread_
    void accept_loop();

    // Send the entire WAL file to a newly connected replica.
    // Called under replicas_mutex_ to prevent missed entries.
    void send_full_sync(int fd);

    // Write exactly len bytes to fd, handling partial writes.
    static bool write_all(int fd, const uint8_t* buf, std::size_t len);
};

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaClient — Replica-side leader connection
// ─────────────────────────────────────────────────────────────────────────────
// Connects to the leader's replication port and receives a continuous
// stream of WAL entries (initial full sync + live updates).
//
// Each received entry is parsed and applied to the local Store<>.
// CRC32 is verified for every entry — corrupted entries cause
// disconnection (same as WAL replay semantics).

class ReplicaClient {
public:
    ReplicaClient(const std::string& leader_host, uint16_t leader_port,
                  Store<>& store);
    ~ReplicaClient();

    // Non-copyable
    ReplicaClient(const ReplicaClient&) = delete;
    ReplicaClient& operator=(const ReplicaClient&) = delete;

    // Connect to the leader and start receiving WAL entries.
    // Spawns a background thread for the receive loop.
    // Returns empty string on success, error message on failure.
    [[nodiscard]] std::string start();

    // Disconnect from the leader.
    void stop();

    // Number of entries received and applied (for monitoring / testing).
    std::size_t entries_received() const { return entries_received_.load(); }

private:
    std::string leader_host_;
    uint16_t leader_port_;
    Store<>& store_;

    int fd_ = -1;
    std::thread recv_thread_;
    std::atomic<bool> running_{false};
    std::atomic<std::size_t> entries_received_{0};

    // Background receive loop — runs in recv_thread_
    void recv_loop();

    // Read exactly len bytes from fd_, handling partial reads.
    // Returns false on EOF or error.
    bool read_exact(uint8_t* buf, std::size_t len);
};
