# kvstore — Distributed Key-Value Store in C++20

A high-performance, distributed key-value store built from scratch in C++20, targeting Linux. Designed to demonstrate deep systems engineering knowledge: async I/O, lock-free data structures, binary protocols, crash recovery, and replication.

## Architecture

The system is built in 5 layers, each adding a critical capability:

| Layer | Component | Status |
|-------|-----------|--------|
| 1 | Async TCP Server (epoll, edge-triggered) | ✅ Complete |
| 2 | RESP Protocol Parser + Command Handler | ✅ Complete |
| 3 | Sharded Concurrent Hash Map | ✅ Complete |
| 4 | Write-Ahead Log (WAL) + Crash Recovery | ✅ Complete |
| 5 | Single-Leader Replication | ✅ Complete |

## Performance

Benchmark results (1M operations, 4 threads, Release build):

| Metric | Store\<1\> (1 shard) | Store\<16\> (16 shards) | Speedup |
|--------|---------------------|------------------------|---------|
| SET throughput | 1.95 M ops/sec | 4.69 M ops/sec | **2.40x** |
| GET throughput | 11.42 M ops/sec | 14.40 M ops/sec | **1.26x** |
| SET p99 latency | 12,285 ns | 2,856 ns | **4.3x better** |

## Building

```bash
# Create build directory and configure
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Running

```bash
# Start the server (default port 6379)
./kvstore

# Start on a custom port
./kvstore --port 8080

# Configure sync policy (every, periodic, none)
./kvstore --sync every      # fdatasync after every write (default, safest)
./kvstore --sync periodic   # fdatasync every 100ms (balanced)
./kvstore --sync none       # no explicit sync (fastest, for testing)

# Use a custom WAL file path
./kvstore --wal /path/to/my.wal

# Run as leader (accepts writes, replicates to followers)
./kvstore --role leader --repl-port 6380

# Run as replica (read-only, receives data from leader)
./kvstore --role replica --port 6381 --leader-host 127.0.0.1 --leader-port 6380
```

## Testing

```bash
# Run all tests
./build/test_protocol    # 31 protocol tests
./build/test_store       # 18 store tests (incl. thread safety)
./build/test_wal         # 13 WAL tests (CRC32, corruption, crash recovery)
./build/test_replication # 6 replication tests (full sync, live, read-only)

# Or use CTest
cd build && ctest --output-on-failure

# Run the benchmark
./build/bench_store

# Interactive testing with netcat
nc localhost 6379
PING
SET mykey myvalue
GET mykey
DEL mykey
```

## Supported Commands

| Command | Example | Response |
|---------|---------|----------|
| `PING` | `PING` | `+PONG\r\n` |
| `PING msg` | `PING hello` | `$5\r\nhello\r\n` |
| `SET key value` | `SET foo bar` | `+OK\r\n` |
| `GET key` | `GET foo` | `$3\r\nbar\r\n` or `$-1\r\n` |
| `DEL key` | `DEL foo` | `:1\r\n` or `:0\r\n` |

Commands are case-insensitive. Multiple commands can be pipelined in a single TCP segment.

## Key Design Features

- **Zero-copy parsing**: `std::string_view` args point into the read buffer — no heap allocation during parsing
- **Sharded storage**: 16-shard concurrent hash map with `alignas(64)` to prevent false sharing
- **Reader-writer locking**: `std::shared_mutex` allows concurrent GETs on the same shard
- **Transparent hash**: C++20 heterogeneous lookup avoids temporary `std::string` on GET
- **TTL support**: Optional per-key expiry using `steady_clock` with lazy deletion
- **Edge-triggered epoll**: Kernel-level I/O multiplexing with `accept4` for atomic non-blocking
- **Write-Ahead Log**: Binary WAL with CRC32 for crash recovery — data survives server restarts
- **Crash recovery**: WAL replay on startup rebuilds exact in-memory state
- **Configurable durability**: `fdatasync` per-write, periodic, or none via `--sync` flag
- **Compile-time CRC32**: 1KB lookup table generated at compile time via `constexpr`
- **Single-leader replication**: Async WAL streaming from leader to replicas
- **Full sync**: New replicas automatically receive entire WAL state on connect
- **Read-only replicas**: Serve GET/PING, reject SET/DEL with READONLY error
- **Horizontal read scaling**: Distribute GET load across multiple replicas

## Requirements

- **OS**: Linux (Ubuntu 22.04 / 24.04)
- **Compiler**: GCC 12+ with C++20 support
- **Build System**: CMake 3.20+
- **Dependencies**: None (pure Linux syscalls, no third-party libraries)

## Project Structure

```
kvstore/
├── src/
│   ├── main.cpp           # Entry point, CLI parsing, role-based startup
│   ├── server.h            # Async TCP server declaration
│   ├── server.cpp          # epoll event loop implementation
│   ├── protocol.h          # RESP parser, formatters, command handler
│   ├── protocol.cpp        # Protocol + WAL-first writes + replication
│   ├── store.h             # Sharded concurrent hash map declaration
│   ├── store.cpp           # Store implementation + explicit instantiations
│   ├── wal.h               # Write-Ahead Log declaration + binary format
│   ├── wal.cpp             # WAL implementation + CRC32
│   ├── replication.h       # ReplicationManager + ReplicaClient
│   └── replication.cpp     # Leader broadcast, full sync, replica receive
├── tests/
│   ├── test_protocol.cpp   # 31 protocol unit tests
│   ├── test_store.cpp      # 18 store unit tests (thread safety, TTL)
│   ├── test_wal.cpp        # 13 WAL unit tests (CRC32, corruption, recovery)
│   └── test_replication.cpp# 6 replication integration tests
├── bench/
│   └── bench_store.cpp     # 1M ops benchmark: 1-shard vs 16-shard
├── docs/
│   ├── DESIGN_LAYER1.md    # Layer 1 design decisions
│   ├── DESIGN_LAYER2.md    # Layer 2 design decisions
│   ├── DESIGN_LAYER3.md    # Layer 3 design decisions
│   ├── DESIGN_LAYER4.md    # Layer 4 design decisions
│   └── DESIGN_LAYER5.md    # Layer 5 design decisions
├── CMakeLists.txt          # Build configuration
├── README.md               # This file
└── .gitignore
```

## Design Documents

Each layer has a detailed design document explaining every decision, alternatives considered, and trade-offs:

- [Layer 1 — Async TCP Server](docs/DESIGN_LAYER1.md)
- [Layer 2 — RESP Protocol Parser](docs/DESIGN_LAYER2.md)
- [Layer 3 — Sharded Concurrent Hash Map](docs/DESIGN_LAYER3.md)
- [Layer 4 — Write-Ahead Log + Crash Recovery](docs/DESIGN_LAYER4.md)
- [Layer 5 — Single-Leader Replication](docs/DESIGN_LAYER5.md)

## License

This project is for educational and portfolio purposes.

