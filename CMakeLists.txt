cmake_minimum_required(VERSION 3.20)

# ──────────────────────────────────────────────────────────────────────────────
# Project: kvstore — A distributed key-value store built in C++20
# Layer 5: Single-Leader Replication
# ──────────────────────────────────────────────────────────────────────────────
project(kvstore
    VERSION 0.5.0
    LANGUAGES CXX
    DESCRIPTION "High-performance distributed key-value store in C++20"
)

# ── Compiler requirements ────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Warning flags ────────────────────────────────────────────────────────────
add_compile_options(-Wall -Wextra -Werror -Wpedantic)

# ── Optimization flags for Release builds ────────────────────────────────────
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

# ── Threading ────────────────────────────────────────────────────────────────
# Required for std::shared_mutex, std::thread, replication threads.
find_package(Threads REQUIRED)

# ── Enable CTest ─────────────────────────────────────────────────────────────
enable_testing()

# ── Main server binary ──────────────────────────────────────────────────────
add_executable(kvstore
    src/main.cpp
    src/server.cpp
    src/protocol.cpp
    src/store.cpp
    src/wal.cpp
    src/replication.cpp
)
target_include_directories(kvstore PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kvstore Threads::Threads)

# ── Protocol unit tests ─────────────────────────────────────────────────────
add_executable(test_protocol
    tests/test_protocol.cpp
    src/protocol.cpp
    src/store.cpp
    src/wal.cpp
    src/replication.cpp
)
target_include_directories(test_protocol PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_protocol Threads::Threads)
add_test(NAME test_protocol COMMAND test_protocol)

# ── Store unit tests ─────────────────────────────────────────────────────────
add_executable(test_store
    tests/test_store.cpp
    src/store.cpp
)
target_include_directories(test_store PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_store Threads::Threads)
add_test(NAME test_store COMMAND test_store)

# ── WAL unit tests ───────────────────────────────────────────────────────────
add_executable(test_wal
    tests/test_wal.cpp
    src/wal.cpp
)
target_include_directories(test_wal PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_wal Threads::Threads)
add_test(NAME test_wal COMMAND test_wal)

# ── Replication integration tests ───────────────────────────────────────────
add_executable(test_replication
    tests/test_replication.cpp
    src/protocol.cpp
    src/store.cpp
    src/wal.cpp
    src/replication.cpp
)
target_include_directories(test_replication PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_replication Threads::Threads)
add_test(NAME test_replication COMMAND test_replication)

# ── Store benchmark ──────────────────────────────────────────────────────────
add_executable(bench_store
    bench/bench_store.cpp
    src/store.cpp
)
target_include_directories(bench_store PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(bench_store Threads::Threads)
