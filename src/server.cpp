// ─────────────────────────────────────────────────────────────────────────────
// server.cpp — Implementation of the epoll-based async TCP server
//
// This file contains the core event loop and all socket I/O logic.
// Every syscall is checked for errors and logged to stderr with errno.
// ─────────────────────────────────────────────────────────────────────────────

#include "server.h"

#include <sys/epoll.h>      // epoll_create1, epoll_ctl, epoll_wait
#include <sys/socket.h>     // socket, setsockopt, bind, listen, accept4
#include <netinet/in.h>     // sockaddr_in, INADDR_ANY
#include <netinet/tcp.h>    // TCP_NODELAY
#include <unistd.h>         // close, read, write
#include <fcntl.h>          // fcntl, F_GETFL, F_SETFL, O_NONBLOCK
#include <cerrno>           // errno
#include <cstring>          // strerror, memset
#include <cstdio>           // fprintf, stderr

// ─────────────────────────────────────────────────────────────────────────────
// Logging helpers
// ─────────────────────────────────────────────────────────────────────────────
// Why macros instead of a logging class?
//   - Zero overhead when compiled out (unlike virtual dispatch in a logger)
//   - __FILE__ and __LINE__ are captured at the call site, not inside a wrapper
//   - For a systems project, fprintf(stderr, ...) is the gold standard:
//     it's async-signal-safe, unbuffered, and always available
//
// We prefix each message with [SERVER] so that in later layers (replication),
// we can distinguish which component is logging.

#define LOG_ERR(fmt, ...) \
    fprintf(stderr, "[SERVER] ERROR: " fmt " (errno=%d: %s)\n", \
            ##__VA_ARGS__, errno, strerror(errno))

#define LOG_INFO(fmt, ...) \
    fprintf(stderr, "[SERVER] INFO: " fmt "\n", ##__VA_ARGS__)

// ─────────────────────────────────────────────────────────────────────────────
// Constructor & Destructor
// ─────────────────────────────────────────────────────────────────────────────

Server::Server(uint16_t port)
    : port_(port) {
    // Nothing else here — actual resource allocation happens in run()
    // so we can return errors instead of throwing from the constructor.
    // This is a deliberate C++ design choice: constructors that can fail
    // either throw (banned on hot path) or use a two-phase init pattern.
    // We use two-phase init (construct + run) to keep the error path clean.
}

Server::~Server() {
    // Close all client connections first, then the listener and epoll fd.
    // Order matters: closing epoll_fd first would leave client fds dangling
    // in the kernel's epoll interest list (harmless but untidy).
    for (auto& [fd, conn] : connections_) {
        close(fd);  // Ignoring close() errors here — we're shutting down
    }

    if (listen_fd_ != -1) {
        close(listen_fd_);
    }
    if (epoll_fd_ != -1) {
        close(epoll_fd_);
    }

    // Free the epoll events buffer that we heap-allocated in create_epoll()
    delete[] events_;
}

// ─────────────────────────────────────────────────────────────────────────────
// Public interface
// ─────────────────────────────────────────────────────────────────────────────

void Server::set_on_message(MessageCallback cb) {
    on_message_ = std::move(cb);
}

std::string Server::run() {
    // ── Phase 1: Setup ──────────────────────────────────────────────────
    // Create listener socket and epoll instance. If either fails, we return
    // immediately with an error string — no partial cleanup needed because
    // the destructor handles all owned resources via RAII.
    if (auto err = create_listener(); !err.empty()) {
        return err;
    }
    if (auto err = create_epoll(); !err.empty()) {
        return err;
    }

    LOG_INFO("Listening on port %u", static_cast<unsigned>(port_));

    // ── Phase 2: Event loop ─────────────────────────────────────────────
    // This is the heart of the server. We block on epoll_wait() until at
    // least one fd has an event, then dispatch each event.
    //
    // Why an infinite loop?
    //   - A server's job is to run forever. Graceful shutdown (via signals)
    //     will be added in a later layer.
    //
    // Why -1 timeout?
    //   - Block indefinitely. We have nothing useful to do between events.
    //   - A timeout would cause a busy-wait loop wasting CPU cycles.
    //     In HFT, you'd use a timeout of 0 (busy-poll) to minimize latency
    //     at the cost of 100% CPU — but that's a different design point.
    for (;;) {
        // epoll_wait returns the number of ready file descriptors.
        // On signal interruption (EINTR), nfds is -1 — we just retry.
        int nfds = epoll_wait(epoll_fd_, events_, kMaxEvents, /*timeout=*/-1);

        if (nfds == -1) {
            if (errno == EINTR) {
                // A signal interrupted us (e.g., SIGCHLD, SIGWINCH).
                // This is normal and harmless — just retry.
                continue;
            }
            LOG_ERR("epoll_wait failed");
            return "epoll_wait failed";
        }

        // Dispatch each ready event to the appropriate handler.
        for (int i = 0; i < nfds; ++i) {
            int fd = events_[i].data.fd;
            uint32_t ev = events_[i].events;

            if (fd == listen_fd_) {
                // New incoming connection(s) on the listener socket.
                // With edge-triggered epoll, we must accept ALL pending
                // connections in a loop (handled inside handle_accept).
                handle_accept();
            } else {
                // ── Error / hangup detection ────────────────────────
                // EPOLLERR: An error occurred on the fd (e.g., RST received)
                // EPOLLHUP: The peer closed the connection (both read+write)
                //
                // We check these BEFORE attempting read/write because
                // reading from an errored socket can return garbage.
                if (ev & (EPOLLERR | EPOLLHUP)) {
                    // Log only if it's an actual error, not a clean disconnect
                    if (ev & EPOLLERR) {
                        LOG_ERR("Error on fd %d", fd);
                    }
                    close_connection(fd);
                    continue;
                }

                // ── Read path ───────────────────────────────────────
                // EPOLLIN: Data is available to read.
                // We handle read before write because a read may generate
                // response data that we then want to write immediately.
                if (ev & EPOLLIN) {
                    handle_read(fd);
                }

                // ── Write path ──────────────────────────────────────
                // EPOLLOUT: The socket's send buffer has space.
                // We only register for EPOLLOUT when we have data to send
                // (see handle_read), to avoid spurious wakeups.
                //
                // Check if connection still exists — handle_read may have
                // closed it if the peer disconnected.
                if ((ev & EPOLLOUT) && connections_.count(fd)) {
                    handle_write(fd);
                }
            }
        }
    }

    // Unreachable — the loop only exits on fatal error (returns above).
    // This return satisfies the compiler's "all paths return" check.
    return "";
}

// ─────────────────────────────────────────────────────────────────────────────
// create_listener — Set up the TCP listening socket
// ─────────────────────────────────────────────────────────────────────────────

std::string Server::create_listener() {
    // ── Create socket ───────────────────────────────────────────────────
    // AF_INET:      IPv4 (IPv6 would be AF_INET6 — not needed for a demo)
    // SOCK_STREAM:  TCP (reliable, ordered byte stream)
    // SOCK_NONBLOCK: Make the socket non-blocking at creation time.
    //   This is a Linux extension (since 2.6.27) that avoids a separate
    //   fcntl() call. We use it on the listener to ensure accept4() doesn't
    //   block when no connections are pending.
    // SOCK_CLOEXEC: Close this fd automatically on exec().
    //   Prevents fd leaks if we ever fork+exec a child process.
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ == -1) {
        LOG_ERR("socket() failed");
        return "Failed to create listener socket";
    }

    // ── Socket options ──────────────────────────────────────────────────
    int opt = 1;

    // SO_REUSEADDR: Allow binding to a port that's in TIME_WAIT state.
    //   When we restart the server, the OS keeps the old socket in TIME_WAIT
    //   for ~60 seconds (2×MSL). Without this flag, bind() would fail with
    //   EADDRINUSE during that window. Every production server sets this.
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        LOG_ERR("setsockopt(SO_REUSEADDR) failed on listener fd=%d", listen_fd_);
        return "Failed to set SO_REUSEADDR";
    }

    // SO_REUSEPORT: Allow multiple sockets to bind to the same port.
    //   The kernel distributes incoming connections across all sockets bound
    //   to the port (since Linux 3.9). This enables a multi-process
    //   architecture where each process has its own accept loop — used by
    //   Nginx and Envoy for horizontal scaling without a thundering-herd.
    //   We set it now so the infrastructure is ready for multi-process later.
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        LOG_ERR("setsockopt(SO_REUSEPORT) failed on listener fd=%d", listen_fd_);
        return "Failed to set SO_REUSEPORT";
    }

    // ── Bind ────────────────────────────────────────────────────────────
    // INADDR_ANY (0.0.0.0): Listen on all network interfaces.
    //   In production, you'd bind to a specific IP for security.
    // htons(): Convert port from host byte order to network byte order
    //   (big-endian). x86 is little-endian, so this byte-swaps.
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == -1) {
        LOG_ERR("bind() failed on port %u", static_cast<unsigned>(port_));
        return "Failed to bind to port";
    }

    // ── Listen ──────────────────────────────────────────────────────────
    // SOMAXCONN: The kernel's maximum backlog size (typically 4096 on Linux).
    //   The backlog is the queue of completed TCP handshakes waiting for
    //   accept(). If this queue fills up, the kernel drops new SYN packets
    //   (the client sees a timeout). SOMAXCONN tells the kernel "use the
    //   max you support" — the safest default for any server.
    if (listen(listen_fd_, SOMAXCONN) == -1) {
        LOG_ERR("listen() failed on fd=%d", listen_fd_);
        return "Failed to listen";
    }

    return "";  // Success
}

// ─────────────────────────────────────────────────────────────────────────────
// create_epoll — Initialize the epoll instance and add the listener fd
// ─────────────────────────────────────────────────────────────────────────────

std::string Server::create_epoll() {
    // epoll_create1(EPOLL_CLOEXEC): Create an epoll instance.
    //   EPOLL_CLOEXEC closes the epoll fd on exec(), same reason as
    //   SOCK_CLOEXEC above. We use epoll_create1 (not epoll_create)
    //   because epoll_create's "size" parameter has been ignored since
    //   Linux 2.6.8 — it exists only for backward compatibility.
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        LOG_ERR("epoll_create1() failed");
        return "Failed to create epoll instance";
    }

    // Allocate the events buffer for epoll_wait results.
    // We heap-allocate rather than stack-allocate because:
    //   1. struct epoll_event is 12 bytes × 64 = 768 bytes — fits on stack,
    //      but heap gives us flexibility to resize later without changing
    //      the event loop code.
    //   2. The buffer lives for the lifetime of the server, not a single
    //      function call, so heap ownership is semantically correct.
    events_ = new struct epoll_event[kMaxEvents];

    // Add the listener fd to epoll, watching for incoming connections.
    // EPOLLIN: Notify us when a new connection is pending (accept-ready).
    // EPOLLET: Edge-triggered mode — we must accept in a loop until EAGAIN.
    if (!epoll_add(listen_fd_, EPOLLIN | EPOLLET)) {
        return "Failed to add listener to epoll";
    }

    return "";  // Success
}

// ─────────────────────────────────────────────────────────────────────────────
// handle_accept — Accept all pending connections (edge-triggered drain)
// ─────────────────────────────────────────────────────────────────────────────

void Server::handle_accept() {
    // Edge-triggered semantics: epoll only notifies us ONCE when new
    // connections arrive. If 5 clients connect between two epoll_wait
    // calls, we get ONE event. We must loop and accept() until we get
    // EAGAIN (no more pending connections).
    //
    // If we accepted only one connection per event, the other 4 would be
    // stranded until the next unrelated event wakes epoll_wait.
    for (;;) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        // accept4: Linux extension that atomically sets SOCK_NONBLOCK and
        // SOCK_CLOEXEC on the new socket. Using accept() + fcntl() would
        // create a race window where the fd exists but isn't non-blocking.
        int client_fd = accept4(
            listen_fd_,
            reinterpret_cast<struct sockaddr*>(&client_addr),
            &addr_len,
            SOCK_NONBLOCK | SOCK_CLOEXEC
        );

        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more pending connections — this is the normal exit
                // path for the edge-triggered accept loop.
                break;
            }
            // Genuine error (e.g., EMFILE = too many open files).
            // Log and continue — don't crash the server for one bad accept.
            LOG_ERR("accept4() failed");
            break;
        }

        // ── Set TCP_NODELAY on the client socket ────────────────────
        // TCP_NODELAY disables Nagle's algorithm, which buffers small
        // writes and coalesces them into fewer TCP segments. Nagle reduces
        // overhead for bulk transfers but adds up to 40ms of latency for
        // request-response protocols like ours.
        //
        // In HFT, Nagle is the #1 cause of unexpected latency spikes
        // in TCP communication. Always disable it for interactive protocols.
        int opt = 1;
        if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == -1) {
            LOG_ERR("setsockopt(TCP_NODELAY) failed on client fd=%d", client_fd);
            // Non-fatal: we continue even if TCP_NODELAY fails.
            // The connection will work, just with higher latency.
        }

        // ── Create Connection object and register with epoll ────────
        // emplace constructs the Connection in-place inside the map,
        // avoiding a copy. The Connection's read_buf is a fixed-size
        // array inside the struct, so this is one allocation for the
        // map node (the struct itself is embedded in the node).
        auto [it, inserted] = connections_.emplace(client_fd, Connection{});
        if (!inserted) {
            // This should never happen: the fd is freshly returned by the
            // kernel. If it's already in our map, something is very wrong.
            LOG_ERR("Duplicate fd %d in connection map!", client_fd);
            close(client_fd);
            continue;
        }

        it->second.fd = client_fd;

        // Initialize read buffer to zero. While not strictly necessary
        // (we track valid bytes via read_offset), zeroing prevents
        // information leaks and makes debugging easier.
        memset(it->second.read_buf, 0, Connection::kReadBufSize);

        // Register the client fd with epoll for read events.
        // EPOLLIN | EPOLLET: Notify on incoming data, edge-triggered.
        // We do NOT register EPOLLOUT yet — we only want write
        // notifications when we actually have data to send. Registering
        // EPOLLOUT prematurely causes constant spurious wakeups because
        // the send buffer is almost always writable.
        if (!epoll_add(client_fd, EPOLLIN | EPOLLET)) {
            LOG_ERR("Failed to add client fd=%d to epoll", client_fd);
            close_connection(client_fd);
            continue;
        }

        LOG_INFO("Accepted connection: fd=%d", client_fd);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// handle_read — Read data from a client connection (edge-triggered drain)
// ─────────────────────────────────────────────────────────────────────────────

void Server::handle_read(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        // This can happen if a previous event in the same epoll_wait batch
        // already closed this connection. Not an error.
        return;
    }

    Connection& conn = it->second;

    // Edge-triggered: we must read ALL available data until EAGAIN.
    // If we stop early, epoll won't notify us again until NEW data arrives
    // (not the leftover data we didn't read).
    for (;;) {
        // Calculate remaining buffer space.
        std::size_t remaining = Connection::kReadBufSize - conn.read_offset;

        if (remaining == 0) {
            // Buffer is full. This means the client sent > 4KB without a
            // complete command, or we haven't processed previous data.
            // For now, we close the connection to prevent memory issues.
            // A production server would either:
            //   (a) Grow the buffer dynamically (more memory, more latency)
            //   (b) Return an error response to the client
            LOG_ERR("Read buffer full for fd=%d, closing connection", fd);
            close_connection(fd);
            return;
        }

        // read() into the buffer starting at the current offset.
        // We read into [read_offset, kReadBufSize) — the unused portion.
        ssize_t n = read(fd, conn.read_buf + conn.read_offset, remaining);

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more data available right now. This is the normal
                // exit for edge-triggered reads. All available data has
                // been consumed — epoll will re-notify us when more arrives.
                break;
            }
            if (errno == EINTR) {
                // A signal interrupted the read. Retry immediately.
                continue;
            }
            // Genuine read error (e.g., ECONNRESET, EIO).
            LOG_ERR("read() failed on fd=%d", fd);
            close_connection(fd);
            return;
        }

        if (n == 0) {
            // EOF: The peer closed the connection gracefully (sent FIN).
            // read() returns 0 exactly once per connection — after this,
            // the fd is half-closed (we can still write but not read).
            // We close fully because we have nothing to send after the
            // client disconnects.
            LOG_INFO("Peer disconnected: fd=%d", fd);
            close_connection(fd);
            return;
        }

        // ── Successful read of n bytes ──────────────────────────────
        // Advance the read offset to reflect the new data.
        conn.read_offset += static_cast<std::size_t>(n);

        // Invoke the user callback with the ENTIRE buffer contents.
        // Why the entire buffer and not just the new bytes?
        //   Because a command might span multiple reads (partial read).
        //   The callback needs to see all accumulated bytes to determine
        //   if a complete command is available.
        //
        // Buffer management contract:
        //   The callback is responsible for:
        //   1. Parsing as many complete commands as possible
        //   2. Compacting the buffer (memmove remaining bytes to front)
        //   3. Updating conn.read_offset to reflect remaining bytes
        //   This allows partial commands to survive across reads — the
        //   unconsumed bytes stay at the front of the buffer, and the
        //   next read() appends after them.
        if (on_message_) {
            on_message_(conn, conn.read_buf, conn.read_offset);
        }
    }

    // After reading, if we have data to send (the callback appended to
    // write_buf), register for EPOLLOUT so epoll notifies us when the
    // socket is writable.
    if (it != connections_.end() && !conn.write_buf.empty()) {
        // EPOLLIN | EPOLLOUT | EPOLLET: Watch for both read and write.
        // We keep EPOLLIN because more data might arrive while we're
        // draining the write buffer.
        if (!epoll_mod(fd, EPOLLIN | EPOLLOUT | EPOLLET)) {
            LOG_ERR("Failed to mod epoll for write on fd=%d", fd);
            close_connection(fd);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// handle_write — Drain the write buffer to the socket
// ─────────────────────────────────────────────────────────────────────────────

void Server::handle_write(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }

    Connection& conn = it->second;

    // Edge-triggered: drain the write buffer completely (until EAGAIN).
    while (conn.write_offset < conn.write_buf.size()) {
        // write() as many bytes as possible from the unsent portion.
        ssize_t n = write(
            fd,
            conn.write_buf.data() + conn.write_offset,
            conn.write_buf.size() - conn.write_offset
        );

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket send buffer is full. The kernel can't accept more
                // data right now. epoll will notify us again when space
                // opens up (the kernel ACKs some bytes and frees buffer).
                return;
            }
            if (errno == EINTR) {
                // Signal interrupted us — retry immediately.
                continue;
            }
            // Genuine write error (e.g., EPIPE = peer closed, ECONNRESET).
            LOG_ERR("write() failed on fd=%d", fd);
            close_connection(fd);
            return;
        }

        // Advance the write offset by the number of bytes the kernel accepted.
        conn.write_offset += static_cast<std::size_t>(n);
    }

    // ── All data sent ───────────────────────────────────────────────────
    // Clear the write buffer and offset to reclaim memory.
    conn.write_buf.clear();
    conn.write_offset = 0;

    // Stop watching for EPOLLOUT. If we leave it registered, epoll will
    // fire continuously because the socket is almost always writable
    // (the send buffer is typically 128KB+ on Linux). This would cause
    // a busy-spin consuming 100% CPU.
    if (!epoll_mod(fd, EPOLLIN | EPOLLET)) {
        LOG_ERR("Failed to mod epoll after write drain on fd=%d", fd);
        close_connection(fd);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// close_connection — Clean teardown of a client connection
// ─────────────────────────────────────────────────────────────────────────────

void Server::close_connection(int fd) {
    // Note: We do NOT need to explicitly call epoll_ctl(EPOLL_CTL_DEL)
    // before close(). When the last file descriptor referring to an
    // underlying open file description is closed, epoll automatically
    // removes it from the interest list. This is guaranteed by the kernel
    // since Linux 2.6.9.
    //
    // However, if we had dup()'d the fd, we WOULD need EPOLL_CTL_DEL
    // because closing one copy doesn't affect the other. We don't dup(),
    // so we're safe.

    close(fd);

    // Remove from our connection map. This destroys the Connection struct,
    // freeing the write_buf string (the read_buf is embedded, no free needed).
    connections_.erase(fd);

    LOG_INFO("Closed connection: fd=%d", fd);
}

// ─────────────────────────────────────────────────────────────────────────────
// set_nonblocking — Make a file descriptor non-blocking
// ─────────────────────────────────────────────────────────────────────────────

bool Server::set_nonblocking(int fd) {
    // Get current flags, add O_NONBLOCK, set flags.
    // We must preserve existing flags (e.g., O_CLOEXEC) — don't just set
    // O_NONBLOCK alone, that would clear everything else.
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        LOG_ERR("fcntl(F_GETFL) failed on fd=%d", fd);
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG_ERR("fcntl(F_SETFL, O_NONBLOCK) failed on fd=%d", fd);
        return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// epoll_add — Register an fd with the epoll instance
// ─────────────────────────────────────────────────────────────────────────────

bool Server::epoll_add(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;  // Store the fd in the event data so we can retrieve
                       // it in the event loop (epoll_wait returns this)

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
        LOG_ERR("epoll_ctl(ADD) failed for fd=%d", fd);
        return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// epoll_mod — Modify the event mask for an existing fd in epoll
// ─────────────────────────────────────────────────────────────────────────────

bool Server::epoll_mod(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == -1) {
        LOG_ERR("epoll_ctl(MOD) failed for fd=%d", fd);
        return false;
    }
    return true;
}
