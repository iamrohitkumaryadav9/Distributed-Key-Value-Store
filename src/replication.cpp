// ─────────────────────────────────────────────────────────────────────────────
// replication.cpp — Single-leader replication implementation
//
// Implementation notes:
//   - Leader uses poll() with timeout in the accept loop for clean shutdown
//   - Full sync reads the WAL file under replicas_mutex_ to prevent races
//   - broadcast() removes disconnected replicas via erase-remove pattern
//   - Replica uses blocking read() in the receive loop for simplicity
//   - CRC32 is verified on every received entry (same as WAL replay)
// ─────────────────────────────────────────────────────────────────────────────

#include "replication.h"

#include <arpa/inet.h>    // inet_pton, htons
#include <cerrno>         // errno
#include <cstdio>         // fprintf, stderr
#include <cstring>        // strerror, memcpy, memset
#include <fcntl.h>        // open, O_RDONLY
#include <netinet/in.h>   // sockaddr_in
#include <poll.h>         // poll, POLLIN
#include <sys/socket.h>   // socket, bind, listen, accept4, setsockopt
#include <sys/stat.h>     // fstat
#include <unistd.h>       // close, read, write, pread

// ─────────────────────────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────────────────────────

#define REPL_LOG_INFO(fmt, ...) \
    fprintf(stderr, "[REPL] INFO: " fmt "\n", ##__VA_ARGS__)

#define REPL_LOG_ERR(fmt, ...) \
    fprintf(stderr, "[REPL] ERROR: " fmt " (errno=%d: %s)\n", \
            ##__VA_ARGS__, errno, strerror(errno))

// ═════════════════════════════════════════════════════════════════════════════
// ReplicationManager — Leader side
// ═════════════════════════════════════════════════════════════════════════════

ReplicationManager::ReplicationManager(uint16_t port)
    : port_(port) {
}

ReplicationManager::~ReplicationManager() {
    stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// start — Create listener and spawn accept thread
// ─────────────────────────────────────────────────────────────────────────────

std::string ReplicationManager::start(const std::string& wal_path) {
    wal_path_ = wal_path;

    // Create TCP socket for replication listener
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd_ == -1) {
        return "Failed to create replication socket";
    }

    // SO_REUSEADDR: allow immediate restart after crash
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to replication port on all interfaces
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        close(listen_fd_);
        listen_fd_ = -1;
        return "Failed to bind replication port " + std::to_string(port_);
    }

    // Backlog of 5: we don't expect many simultaneous replica connections
    if (listen(listen_fd_, 5) == -1) {
        close(listen_fd_);
        listen_fd_ = -1;
        return "Failed to listen on replication port";
    }

    // Start accept thread
    running_.store(true);
    accept_thread_ = std::thread(&ReplicationManager::accept_loop, this);

    REPL_LOG_INFO("Leader replication listening on port %u", port_);
    return "";
}

// ─────────────────────────────────────────────────────────────────────────────
// stop — Shut down the listener and disconnect all replicas
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::stop() {
    running_.store(false);

    // Close listener to unblock accept/poll
    if (listen_fd_ != -1) {
        close(listen_fd_);
        listen_fd_ = -1;
    }

    // Join accept thread
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // Disconnect all replicas
    std::lock_guard lock(replicas_mutex_);
    for (int fd : replica_fds_) {
        close(fd);
    }
    replica_fds_.clear();

    REPL_LOG_INFO("Replication stopped");
}

// ─────────────────────────────────────────────────────────────────────────────
// replica_count — Number of connected replicas
// ─────────────────────────────────────────────────────────────────────────────

std::size_t ReplicationManager::replica_count() const {
    std::lock_guard lock(replicas_mutex_);
    return replica_fds_.size();
}

// ─────────────────────────────────────────────────────────────────────────────
// accept_loop — Background thread accepting replica connections
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::accept_loop() {
    while (running_.load()) {
        // poll() with 500ms timeout so we can check running_ periodically.
        // Without this, accept() would block indefinitely and we couldn't
        // shut down cleanly.
        struct pollfd pfd{};
        pfd.fd = listen_fd_;
        pfd.events = POLLIN;

        int ret = poll(&pfd, 1, 500);
        if (ret <= 0) continue;      // Timeout or error — retry
        if (!running_.load()) break;  // Shutdown requested

        // Accept the new replica connection.
        // SOCK_CLOEXEC: close on exec to prevent fd leaks.
        // We use blocking I/O for replica sockets because full sync
        // may need to send large amounts of data.
        int replica_fd = accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (replica_fd == -1) continue;

        // ── Critical section ────────────────────────────────────────
        // Hold replicas_mutex_ during full sync AND adding to the list.
        // This prevents a race where:
        //   1. broadcast() sends entry X to existing replicas
        //   2. New replica connects, misses entry X
        //   3. New replica gets full sync (which might not include X
        //      if X was written after the fstat)
        //
        // By holding the lock, broadcast() blocks during full sync.
        // Any entry written during this time:
        //   - Is in the WAL file (appended before broadcast)
        //   - broadcast() hasn't sent it yet (blocked on mutex)
        //   - Full sync reads the WAL including this entry
        //   - After unlock, broadcast() sends it — but the replica
        //     already has it from full sync. Since SET is idempotent,
        //     applying it twice is harmless.
        {
            std::lock_guard lock(replicas_mutex_);

            // Send the entire WAL file to the new replica
            send_full_sync(replica_fd);

            // Add to active replica list
            replica_fds_.push_back(replica_fd);
        }

        REPL_LOG_INFO("Replica connected: fd=%d (total: %zu)",
                      replica_fd, replica_count());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// send_full_sync — Send the entire WAL file to a replica
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::send_full_sync(int fd) {
    // Open the WAL file for reading
    int wal_fd = ::open(wal_path_.c_str(), O_RDONLY | O_CLOEXEC);
    if (wal_fd == -1) {
        REPL_LOG_ERR("Failed to open WAL for full sync: %s", wal_path_.c_str());
        return;
    }

    // Get current WAL size
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (fstat(wal_fd, &st) == -1 || st.st_size == 0) {
        close(wal_fd);
        REPL_LOG_INFO("Full sync: WAL is empty, nothing to send");
        return;
    }

    // Read and send in chunks.
    // Using pread (positional read) to avoid file position state.
    uint8_t buf[8192];
    off_t offset = 0;
    off_t remaining = st.st_size;

    while (remaining > 0) {
        ssize_t to_read = (remaining > static_cast<off_t>(sizeof(buf)))
                          ? static_cast<ssize_t>(sizeof(buf))
                          : static_cast<ssize_t>(remaining);
        ssize_t n = pread(wal_fd, buf, static_cast<size_t>(to_read), offset);
        if (n <= 0) break;

        if (!write_all(fd, buf, static_cast<size_t>(n))) {
            REPL_LOG_ERR("Full sync write failed at offset %ld", static_cast<long>(offset));
            break;
        }

        offset += n;
        remaining -= n;
    }

    close(wal_fd);
    REPL_LOG_INFO("Full sync complete: sent %ld bytes", static_cast<long>(offset));
}

// ─────────────────────────────────────────────────────────────────────────────
// broadcast — Send a WAL entry to all connected replicas
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::broadcast(const uint8_t* data, uint32_t len) {
    std::lock_guard lock(replicas_mutex_);

    // Iterate with index-based loop so we can erase failed replicas
    auto it = replica_fds_.begin();
    while (it != replica_fds_.end()) {
        if (!write_all(*it, data, len)) {
            // Replica disconnected — clean up
            REPL_LOG_INFO("Replica disconnected: fd=%d", *it);
            close(*it);
            it = replica_fds_.erase(it);
        } else {
            ++it;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// write_all — Write exactly len bytes, handling partial writes
// ─────────────────────────────────────────────────────────────────────────────

bool ReplicationManager::write_all(int fd, const uint8_t* buf, std::size_t len) {
    std::size_t total = 0;
    while (total < len) {
        ssize_t n = ::write(fd, buf + total, len - total);
        if (n <= 0) {
            if (n == -1 && errno == EINTR) continue;
            return false;
        }
        total += static_cast<std::size_t>(n);
    }
    return true;
}

// ═════════════════════════════════════════════════════════════════════════════
// ReplicaClient — Replica side
// ═════════════════════════════════════════════════════════════════════════════

ReplicaClient::ReplicaClient(const std::string& leader_host, uint16_t leader_port,
                             Store<>& store)
    : leader_host_(leader_host)
    , leader_port_(leader_port)
    , store_(store) {
}

ReplicaClient::~ReplicaClient() {
    stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// start — Connect to leader and spawn receive thread
// ─────────────────────────────────────────────────────────────────────────────

std::string ReplicaClient::start() {
    // Create TCP socket
    fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd_ == -1) {
        return "Failed to create replica socket";
    }

    // Resolve leader address
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(leader_port_);

    if (inet_pton(AF_INET, leader_host_.c_str(), &addr.sin_addr) != 1) {
        close(fd_);
        fd_ = -1;
        return "Invalid leader host: " + leader_host_;
    }

    // Connect to leader's replication port
    if (connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        REPL_LOG_ERR("Failed to connect to leader %s:%u",
                     leader_host_.c_str(), leader_port_);
        close(fd_);
        fd_ = -1;
        return "Failed to connect to leader " + leader_host_ + ":" +
               std::to_string(leader_port_);
    }

    // Start receive thread
    running_.store(true);
    recv_thread_ = std::thread(&ReplicaClient::recv_loop, this);

    REPL_LOG_INFO("Connected to leader %s:%u", leader_host_.c_str(), leader_port_);
    return "";
}

// ─────────────────────────────────────────────────────────────────────────────
// stop — Disconnect from leader
// ─────────────────────────────────────────────────────────────────────────────

void ReplicaClient::stop() {
    running_.store(false);

    // Close socket to unblock read()
    if (fd_ != -1) {
        // shutdown() unblocks any blocking read() on this fd
        shutdown(fd_, SHUT_RDWR);
        close(fd_);
        fd_ = -1;
    }

    if (recv_thread_.joinable()) {
        recv_thread_.join();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// recv_loop — Background thread receiving WAL entries from leader
// ─────────────────────────────────────────────────────────────────────────────
// The replication stream is a sequence of WAL entries in the same binary
// format as the on-disk WAL:
//   [entry_len: u32][timestamp: u64][op: u8][key_len: u16][key][val_len: u16][val][crc32: u32]
//
// The receive loop reads entries one by one, verifies CRC32, parses the
// entry, and applies it to the local store.

void ReplicaClient::recv_loop() {
    while (running_.load()) {
        // ── Read entry_len (4 bytes) ────────────────────────────────
        uint32_t entry_len = 0;
        if (!read_exact(reinterpret_cast<uint8_t*>(&entry_len), 4)) {
            if (running_.load()) {
                REPL_LOG_INFO("Leader disconnected or read error");
            }
            break;
        }

        // Validate entry_len
        if (entry_len < wal_format::kMinEntryLen) {
            REPL_LOG_ERR("Invalid entry_len %u from leader", entry_len);
            break;
        }

        // ── Read entry data ─────────────────────────────────────────
        std::vector<uint8_t> data(entry_len);
        if (!read_exact(data.data(), entry_len)) {
            if (running_.load()) {
                REPL_LOG_INFO("Leader disconnected during entry read");
            }
            break;
        }

        // ── Verify CRC32 ────────────────────────────────────────────
        uint32_t payload_size = entry_len - wal_format::kCrc32Size;
        uint32_t stored_crc = 0;
        memcpy(&stored_crc, data.data() + payload_size, 4);
        uint32_t computed_crc = WAL::compute_crc32(data.data(), payload_size);

        if (stored_crc != computed_crc) {
            REPL_LOG_ERR("CRC32 mismatch in replication stream: "
                         "stored=0x%08X computed=0x%08X",
                         stored_crc, computed_crc);
            break;
        }

        // ── Parse entry ─────────────────────────────────────────────
        const uint8_t* p = data.data();

        // Skip timestamp (8 bytes) — not needed for store operations
        p += 8;

        // Op type
        uint8_t op = *p++;

        // Key
        uint16_t key_len = 0;
        memcpy(&key_len, p, 2);
        p += 2;
        std::string key(reinterpret_cast<const char*>(p), key_len);
        p += key_len;

        // Value
        uint16_t val_len = 0;
        memcpy(&val_len, p, 2);
        p += 2;
        std::string value(reinterpret_cast<const char*>(p), val_len);

        // ── Apply to local store ────────────────────────────────────
        if (op == static_cast<uint8_t>(WAL::Entry::OpType::kSet)) {
            store_.set(key, value);
        } else if (op == static_cast<uint8_t>(WAL::Entry::OpType::kDel)) {
            store_.del(key);
        }

        entries_received_.fetch_add(1);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// read_exact — Read exactly len bytes from fd_, handling partial reads
// ─────────────────────────────────────────────────────────────────────────────

bool ReplicaClient::read_exact(uint8_t* buf, std::size_t len) {
    std::size_t total = 0;
    while (total < len) {
        ssize_t n = read(fd_, buf + total, len - total);
        if (n <= 0) {
            if (n == -1 && errno == EINTR) continue;
            return false;  // EOF or error
        }
        total += static_cast<std::size_t>(n);
    }
    return true;
}
