// ─────────────────────────────────────────────────────────────────────────────
// main.cpp — Entry point for the kvstore TCP server
//
// Layer 5: Single-leader replication.
// Roles:
//   standalone (default): Single server, WAL + crash recovery.
//   leader:               Accepts writes, replicates to follower replicas.
//   replica:              Read-only, receives data from leader.
//
// Usage:
//   ./kvstore                                    # standalone (default)
//   ./kvstore --role leader --repl-port 6380     # leader
//   ./kvstore --role replica --leader-host 127.0.0.1 --leader-port 6380 --port 6381
// ─────────────────────────────────────────────────────────────────────────────

#include "server.h"
#include "protocol.h"
#include "replication.h"
#include "wal.h"

#include <cstdio>     // fprintf, stderr
#include <cstdlib>    // EXIT_SUCCESS, EXIT_FAILURE
#include <cstring>    // strlen, strcmp, memmove
#include <csignal>    // signal, SIGPIPE, SIG_IGN
#include <memory>     // std::unique_ptr
#include <string>
#include <string_view>

int main(int argc, char* argv[]) {
    // ── Default configuration ───────────────────────────────────────────
    uint16_t port = 6379;
    std::string wal_path = "kvstore.wal";
    SyncPolicy sync_policy = SyncPolicy::kEveryWrite;

    // Layer 5: Replication configuration
    std::string role = "standalone";   // "standalone", "leader", "replica"
    uint16_t repl_port = 6380;         // Port for replica connections (leader)
    std::string leader_host = "127.0.0.1";  // Leader address (replica)
    uint16_t leader_port = 6380;             // Leader replication port (replica)

    // ── Parse command-line arguments ────────────────────────────────────
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(atoi(argv[i + 1]));
            ++i;
        } else if (strcmp(argv[i], "--wal") == 0 && i + 1 < argc) {
            wal_path = argv[i + 1];
            ++i;
        } else if (strcmp(argv[i], "--sync") == 0 && i + 1 < argc) {
            if (strcmp(argv[i + 1], "every") == 0) {
                sync_policy = SyncPolicy::kEveryWrite;
            } else if (strcmp(argv[i + 1], "periodic") == 0) {
                sync_policy = SyncPolicy::kPeriodic;
            } else if (strcmp(argv[i + 1], "none") == 0) {
                sync_policy = SyncPolicy::kNone;
            } else {
                fprintf(stderr, "[MAIN] Unknown sync policy: %s (use every|periodic|none)\n",
                        argv[i + 1]);
                return EXIT_FAILURE;
            }
            ++i;
        } else if (strcmp(argv[i], "--role") == 0 && i + 1 < argc) {
            role = argv[i + 1];
            if (role != "standalone" && role != "leader" && role != "replica") {
                fprintf(stderr, "[MAIN] Unknown role: %s (use standalone|leader|replica)\n",
                        argv[i + 1]);
                return EXIT_FAILURE;
            }
            ++i;
        } else if (strcmp(argv[i], "--repl-port") == 0 && i + 1 < argc) {
            repl_port = static_cast<uint16_t>(atoi(argv[i + 1]));
            ++i;
        } else if (strcmp(argv[i], "--leader-host") == 0 && i + 1 < argc) {
            leader_host = argv[i + 1];
            ++i;
        } else if (strcmp(argv[i], "--leader-port") == 0 && i + 1 < argc) {
            leader_port = static_cast<uint16_t>(atoi(argv[i + 1]));
            ++i;
        }
    }

    // ── Ignore SIGPIPE ──────────────────────────────────────────────────
    // When we write to a socket whose peer has closed, the kernel sends
    // SIGPIPE to our process. The default handler terminates the program.
    // We ignore it and instead check the return value of write(), which
    // will return -1 with errno = EPIPE.
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "[MAIN] Role: %s | Port: %u\n", role.c_str(), port);

    // ── Create the key-value store ──────────────────────────────────────
    // Store<> is a 16-shard concurrent hash map (default template parameter).
    Store<> store;

    // ── WAL and replication depend on role ───────────────────────────────
    //
    // standalone / leader:
    //   - Create WAL, replay entries into store
    //   - Leader also starts ReplicationManager for replica connections
    //
    // replica:
    //   - No local WAL (replica gets data from leader)
    //   - If the replica crashes, it reconnects and gets a full sync
    //   - Start ReplicaClient to connect to leader
    //
    // Why no WAL on replicas?
    //   A replica WAL would provide local crash recovery, but adds
    //   complexity (double-writing). Since replicas can always get a
    //   full sync from the leader, the simpler approach is preferred
    //   for this layer. Production systems (e.g., Redis) DO write a
    //   local AOF on replicas, but that's a Layer 6+ concern.

    std::unique_ptr<WAL> wal;
    std::unique_ptr<ReplicationManager> repl_mgr;
    std::unique_ptr<ReplicaClient> repl_client;

    if (role == "standalone" || role == "leader") {
        // ── Create the Write-Ahead Log ──────────────────────────────
        wal = std::make_unique<WAL>(wal_path, sync_policy);
        {
            std::string err = wal->open();
            if (!err.empty()) {
                fprintf(stderr, "[MAIN] Fatal: %s\n", err.c_str());
                return EXIT_FAILURE;
            }
        }

        // ── Replay WAL to rebuild in-memory state ───────────────────
        std::size_t replayed = wal->replay([&store](const WAL::Entry& entry) {
            switch (entry.op) {
                case WAL::Entry::OpType::kSet:
                    store.set(entry.key, entry.value);
                    break;
                case WAL::Entry::OpType::kDel:
                    store.del(entry.key);
                    break;
            }
        });

        if (replayed > 0) {
            fprintf(stderr, "[MAIN] Recovered %zu entries from WAL\n", replayed);
        }

        // ── Start replication manager (leader only) ─────────────────
        // The replication manager listens on a separate port for
        // replica connections. When a replica connects, it receives
        // the entire WAL file (full sync), then receives live updates
        // as they happen.
        if (role == "leader") {
            repl_mgr = std::make_unique<ReplicationManager>(repl_port);
            std::string err = repl_mgr->start(wal->path());
            if (!err.empty()) {
                fprintf(stderr, "[MAIN] Fatal: %s\n", err.c_str());
                return EXIT_FAILURE;
            }
        }

    } else if (role == "replica") {
        // ── Start replica client ────────────────────────────────────
        // The replica connects to the leader's replication port and
        // receives WAL entries (first a full sync, then live updates).
        // Entries are applied to the local store as they arrive.
        repl_client = std::make_unique<ReplicaClient>(
            leader_host, leader_port, store);
        std::string err = repl_client->start();
        if (!err.empty()) {
            fprintf(stderr, "[MAIN] Fatal: %s\n", err.c_str());
            return EXIT_FAILURE;
        }
    }

    // ── Create and configure the server ─────────────────────────────────
    Server server(port);

    // ── Determine if this server is read-only ───────────────────────────
    // Replicas reject SET/DEL from clients. They only receive writes
    // from the leader's replication stream.
    const bool read_only = (role == "replica");

    // ── Get raw pointers for the lambda capture ─────────────────────────
    // unique_ptr can't be captured by value in lambdas. We capture raw
    // pointers, which are valid for the lifetime of main().
    WAL* wal_ptr = wal.get();
    ReplicationManager* repl_ptr = repl_mgr.get();

    // ── Set the protocol callback ───────────────────────────────────────
    server.set_on_message([&store, wal_ptr, repl_ptr, read_only](
                              Connection& conn,
                              const char* data,
                              std::size_t len) {
        std::string_view buffer(data, len);
        std::size_t total_consumed = 0;

        while (!buffer.empty()) {
            ParseResult result = parse_inline_command(buffer);

            if (result.status == ParseStatus::kIncomplete) {
                break;
            }

            if (result.status == ParseStatus::kError) {
                resp_error(conn.write_buf, "ERR protocol error: too many arguments");
                total_consumed += result.bytes_consumed;
                buffer.remove_prefix(result.bytes_consumed);
                continue;
            }

            if (result.num_args > 0) {
                dispatch_command(result, conn.write_buf, store,
                                 wal_ptr, repl_ptr, read_only);
            }

            total_consumed += result.bytes_consumed;
            buffer.remove_prefix(result.bytes_consumed);
        }

        // ── Buffer compaction ───────────────────────────────────────
        std::size_t remaining = len - total_consumed;
        if (remaining > 0 && total_consumed > 0) {
            memmove(conn.read_buf, data + total_consumed, remaining);
        }
        conn.read_offset = remaining;
    });

    // ── Run the event loop ──────────────────────────────────────────────
    std::string err = server.run();

    if (!err.empty()) {
        fprintf(stderr, "[MAIN] Fatal: %s\n", err.c_str());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
