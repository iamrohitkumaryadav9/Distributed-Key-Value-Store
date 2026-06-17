#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// server.h — Async TCP server using Linux epoll (edge-triggered)
//
// Design:
//   Single-threaded event loop multiplexing I/O over many connections using
//   epoll in edge-triggered mode. This is the same architecture used by Redis,
//   Nginx, and most HFT network stacks where predictable, low-latency I/O
//   matters more than raw throughput across cores.
//
// Why single-threaded?
//   - No lock contention, no context switches, deterministic latency
//   - A single core can saturate a 10 Gbps NIC for small messages
//   - We add threading later (Layer 3) where it actually helps: the data store
//
// Why edge-triggered?
//   - Level-triggered epoll re-reports readiness every epoll_wait() call if
//     the fd is still readable, causing redundant wakeups under load
//   - Edge-triggered only fires when the fd transitions to ready, so we must
//     drain the fd completely (read until EAGAIN) — more code, but fewer
//     syscalls under high connection counts
// ─────────────────────────────────────────────────────────────────────────────

#include <cstdint>         // uint16_t, uint32_t
#include <string>          // std::string
#include <vector>          // std::vector for connection buffers
#include <unordered_map>   // std::unordered_map for fd -> Connection mapping
#include <functional>      // std::function for the message callback

// ── Forward declarations for Linux types (avoid polluting header with
//    <sys/epoll.h> — keep compile times down in large projects) ──────────────
struct epoll_event;

// ── Per-connection state ────────────────────────────────────────────────────
// Each accepted connection gets its own read and write buffers, pre-allocated
// at accept time to avoid heap allocation on the hot path (during reads).
//
// Buffer strategy:
//   We use a fixed 4096-byte read buffer. TCP segments on localhost are
//   typically ≤ 64KB; 4KB is enough for most Redis-like commands and keeps
//   memory usage predictable (1000 connections = ~8 MB total buffer space).
//   If a command spans two TCP segments, we accumulate bytes and parse on
//   the next read — the 'read_offset' field tracks how many valid bytes
//   are currently in the buffer.
struct Connection {
    // ── Constants ───────────────────────────────────────────────────────
    static constexpr std::size_t kReadBufSize = 4096;

    // ── Read buffer ─────────────────────────────────────────────────────
    // Pre-allocated at accept time. read_offset marks the boundary between
    // valid data [0, read_offset) and free space [read_offset, kReadBufSize).
    //
    // Why a raw array instead of std::vector?
    //   std::vector stores { pointer, size, capacity } = 24 bytes of metadata
    //   and requires an indirection to reach the data. A flat array is
    //   cache-line friendly and avoids the heap allocation that vector's
    //   constructor would perform.
    char read_buf[kReadBufSize];
    std::size_t read_offset = 0;  // Bytes of valid data in read_buf

    // ── Write buffer ────────────────────────────────────────────────────
    // Responses are appended here. We drain it into the socket when epoll
    // signals EPOLLOUT. Using std::string here because response sizes are
    // variable and we need dynamic growth — this allocation happens during
    // command processing, not on the hot read path.
    std::string write_buf;
    std::size_t write_offset = 0;  // Bytes already sent from write_buf

    // ── File descriptor ─────────────────────────────────────────────────
    int fd = -1;
};

// ── Server class ────────────────────────────────────────────────────────────
// Encapsulates the epoll event loop, listener socket, and connection map.
//
// Usage:
//   Server server(6379);
//   server.set_on_message([](Connection& conn, const char* data, size_t len) {
//       conn.write_buf.append(data, len);  // echo
//   });
//   server.run();  // blocks forever in the event loop
//
class Server {
public:
    // ── Callback type for received data ─────────────────────────────────
    // Called when complete data is available from a connection.
    // The callback should append response data to conn.write_buf.
    // Parameters:
    //   conn — mutable reference to the connection (to append responses)
    //   data — pointer into conn.read_buf where the new data starts
    //   len  — number of new bytes available
    using MessageCallback = std::function<void(Connection& conn,
                                               const char* data,
                                               std::size_t len)>;

    // ── Constructor / Destructor ────────────────────────────────────────
    // port: TCP port to listen on (typically 6379 to match Redis convention)
    explicit Server(uint16_t port);

    // Destructor closes all fds. Rule of 5: we delete copy/move because
    // the server owns file descriptors and the epoll instance — copying
    // would double-close fds, moving would require careful invalidation.
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    // ── Public interface ────────────────────────────────────────────────
    // set_on_message: Register a callback invoked when data arrives.
    // Must be called before run().
    void set_on_message(MessageCallback cb);

    // run: Enter the event loop. Blocks indefinitely.
    // Returns an error string if setup fails, empty string on clean shutdown.
    [[nodiscard]] std::string run();

private:
    // ── Setup helpers ───────────────────────────────────────────────────
    // Each returns an error message on failure, empty string on success.
    // Using std::string for errors instead of exceptions because:
    //   1. Exceptions are banned on the hot path (unwinding is expensive)
    //   2. Error strings are self-documenting and grep-friendly in logs
    [[nodiscard]] std::string create_listener();
    [[nodiscard]] std::string create_epoll();

    // ── Event handlers ──────────────────────────────────────────────────
    void handle_accept();           // New connection on listener fd
    void handle_read(int fd);       // Data available on client fd
    void handle_write(int fd);      // Socket writable, drain write_buf
    void close_connection(int fd);  // Clean teardown of a connection

    // ── Helpers ─────────────────────────────────────────────────────────
    // Sets a file descriptor to non-blocking mode via fcntl(F_SETFL).
    // Returns true on success, false on failure (logs to stderr).
    bool set_nonblocking(int fd);

    // Adds an fd to the epoll instance with the given event mask.
    // Returns true on success, false on failure.
    bool epoll_add(int fd, uint32_t events);

    // Modifies the event mask for an fd already in the epoll set.
    // Returns true on success, false on failure.
    bool epoll_mod(int fd, uint32_t events);

    // ── Member data ─────────────────────────────────────────────────────
    uint16_t port_;                                   // TCP listen port
    int listen_fd_  = -1;                             // Listener socket fd
    int epoll_fd_   = -1;                             // epoll instance fd

    // Map from file descriptor → Connection.
    // Why unordered_map and not a flat array indexed by fd?
    //   - fd values can be sparse (kernel reuses fds, but not contiguously)
    //   - unordered_map gives O(1) average lookup with no wasted memory
    //   - For a production HFT system, you'd use a flat array sized to
    //     the fd limit (ulimit -n) for guaranteed O(1) with no hashing,
    //     but unordered_map is clearer for a portfolio project.
    std::unordered_map<int, Connection> connections_;

    // Pre-allocated buffer for epoll_wait results.
    // 64 events is a balance: too few = more epoll_wait calls under load,
    // too many = wasted stack space. 64 is what Redis uses.
    static constexpr int kMaxEvents = 64;
    struct epoll_event* events_ = nullptr;  // Heap-allocated in create_epoll

    // User-provided callback for processing received data
    MessageCallback on_message_;
};
