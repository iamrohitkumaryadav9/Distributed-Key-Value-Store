// ─────────────────────────────────────────────────────────────────────────────
// bench_store.cpp — Benchmark: 1M GETs and 1M SETs, 1-shard vs 16-shard
//
// Measures per-operation latency (p50, p99) and throughput (ops/sec) for
// the sharded concurrent hash map under multi-threaded contention.
//
// The comparison between Store<1> and Store<16> demonstrates the performance
// benefit of sharding: with a single shard, all threads contend on one mutex;
// with 16 shards, threads operating on different shards never contend.
//
// Methodology:
//   - Pre-populate the store with keys
//   - Launch N threads, each performing ops_per_thread operations
//   - Measure each individual operation latency with steady_clock
//   - Sort all latencies to compute p50 and p99
//   - Compute total throughput as total_ops / elapsed_wall_time
//
// Caveats:
//   - steady_clock::now() itself costs ~20ns on x86, which is significant
//     for operations that complete in ~50-200ns. The reported latencies
//     include this measurement overhead.
//   - Results vary significantly based on CPU, cache topology, and load.
// ─────────────────────────────────────────────────────────────────────────────

#include "store.h"

#include <algorithm>    // std::sort, std::nth_element
#include <chrono>       // std::chrono::steady_clock
#include <cstdio>       // printf
#include <string>       // std::string, std::to_string
#include <thread>       // std::thread
#include <vector>       // std::vector

// ─────────────────────────────────────────────────────────────────────────────
// BenchResult — Holds the outcome of a single benchmark run
// ─────────────────────────────────────────────────────────────────────────────

struct BenchResult {
    double ops_per_sec;   // Total throughput across all threads
    int64_t p50_ns;       // Median per-operation latency (nanoseconds)
    int64_t p99_ns;       // 99th percentile latency (nanoseconds)
    double elapsed_sec;   // Wall-clock time for the entire benchmark
};

// ─────────────────────────────────────────────────────────────────────────────
// compute_percentiles — Sort latencies and extract p50/p99
// ─────────────────────────────────────────────────────────────────────────────

static void compute_percentiles(std::vector<int64_t>& latencies,
                                int64_t& p50, int64_t& p99) {
    if (latencies.empty()) {
        p50 = p99 = 0;
        return;
    }

    // std::sort for accurate percentiles.
    // For production benchmarks, you'd use nth_element for O(n) partial sort,
    // but sort gives us the full distribution for potential histogram analysis.
    std::sort(latencies.begin(), latencies.end());

    std::size_t n = latencies.size();
    p50 = latencies[n * 50 / 100];
    p99 = latencies[n * 99 / 100];
}

// ─────────────────────────────────────────────────────────────────────────────
// bench_set — Benchmark SET operations
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
static BenchResult bench_set(int total_ops, int num_threads) {
    Store<NumShards> store;

    // Each thread gets an equal share of operations
    int ops_per_thread = total_ops / num_threads;

    // Per-thread latency vectors (pre-allocated to avoid realloc during timing)
    std::vector<std::vector<int64_t>> all_latencies(num_threads);
    for (auto& v : all_latencies) {
        v.reserve(ops_per_thread);
    }

    std::vector<std::thread> threads;

    // ── Start timing ────────────────────────────────────────────────────
    auto wall_start = std::chrono::steady_clock::now();

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, &all_latencies, t, ops_per_thread]() {
            auto& latencies = all_latencies[t];

            for (int i = 0; i < ops_per_thread; ++i) {
                // Generate a key that distributes across shards.
                // Using thread_id * ops + i ensures unique keys per thread.
                std::string key = "key:" + std::to_string(t * ops_per_thread + i);
                std::string val = "value:" + std::to_string(i);

                auto t1 = std::chrono::steady_clock::now();
                store.set(key, val);
                auto t2 = std::chrono::steady_clock::now();

                int64_t ns = std::chrono::duration_cast<
                    std::chrono::nanoseconds>(t2 - t1).count();
                latencies.push_back(ns);
            }
        });
    }

    for (auto& t : threads) t.join();

    auto wall_end = std::chrono::steady_clock::now();
    // ── End timing ──────────────────────────────────────────────────────

    double elapsed = std::chrono::duration<double>(wall_end - wall_start).count();

    // Merge per-thread latencies
    std::vector<int64_t> merged;
    merged.reserve(total_ops);
    for (auto& v : all_latencies) {
        merged.insert(merged.end(), v.begin(), v.end());
    }

    BenchResult result{};
    result.elapsed_sec = elapsed;
    result.ops_per_sec = static_cast<double>(merged.size()) / elapsed;
    compute_percentiles(merged, result.p50_ns, result.p99_ns);

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// bench_get — Benchmark GET operations
// ─────────────────────────────────────────────────────────────────────────────

template<std::size_t NumShards>
static BenchResult bench_get(int total_ops, int num_threads) {
    Store<NumShards> store;

    // Pre-populate the store with all keys we'll read
    for (int i = 0; i < total_ops; ++i) {
        store.set("key:" + std::to_string(i), "value:" + std::to_string(i));
    }

    int ops_per_thread = total_ops / num_threads;

    std::vector<std::vector<int64_t>> all_latencies(num_threads);
    for (auto& v : all_latencies) {
        v.reserve(ops_per_thread);
    }

    std::vector<std::thread> threads;

    auto wall_start = std::chrono::steady_clock::now();

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, &all_latencies, t, ops_per_thread]() {
            auto& latencies = all_latencies[t];

            for (int i = 0; i < ops_per_thread; ++i) {
                std::string key = "key:" + std::to_string(t * ops_per_thread + i);

                auto t1 = std::chrono::steady_clock::now();
                auto val = store.get(key);
                auto t2 = std::chrono::steady_clock::now();

                // Prevent the compiler from optimizing away the get() call.
                // volatile prevents dead-code elimination.
                volatile bool has_val = val.has_value();
                (void)has_val;

                int64_t ns = std::chrono::duration_cast<
                    std::chrono::nanoseconds>(t2 - t1).count();
                latencies.push_back(ns);
            }
        });
    }

    for (auto& t : threads) t.join();

    auto wall_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(wall_end - wall_start).count();

    std::vector<int64_t> merged;
    merged.reserve(total_ops);
    for (auto& v : all_latencies) {
        merged.insert(merged.end(), v.begin(), v.end());
    }

    BenchResult result{};
    result.elapsed_sec = elapsed;
    result.ops_per_sec = static_cast<double>(merged.size()) / elapsed;
    compute_percentiles(merged, result.p50_ns, result.p99_ns);

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// print_result — Format and display benchmark results
// ─────────────────────────────────────────────────────────────────────────────

static void print_result(const char* label, const BenchResult& r) {
    printf("  %-28s %8.2f M ops/sec   p50=%5ldns   p99=%5ldns   (%.3fs)\n",
           label,
           r.ops_per_sec / 1e6,
           static_cast<long>(r.p50_ns),
           static_cast<long>(r.p99_ns),
           r.elapsed_sec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

int main() {
    constexpr int TOTAL_OPS = 1'000'000;  // 1M operations
    constexpr int NUM_THREADS = 4;

    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║  kvstore Benchmark: Sharded Concurrent Hash Map                 ║\n");
    printf("║  %d ops, %d threads                                           ║\n",
           TOTAL_OPS, NUM_THREADS);
    printf("╚══════════════════════════════════════════════════════════════════╝\n\n");

    // ── SET Benchmark ───────────────────────────────────────────────────
    printf("=== 1M SET operations (%d threads) ===\n", NUM_THREADS);

    auto set_1  = bench_set<1>(TOTAL_OPS, NUM_THREADS);
    auto set_16 = bench_set<16>(TOTAL_OPS, NUM_THREADS);

    print_result("Store<1>  (1 shard):", set_1);
    print_result("Store<16> (16 shards):", set_16);

    double set_speedup = set_16.ops_per_sec / set_1.ops_per_sec;
    printf("  Throughput speedup: %.2fx\n", set_speedup);

    // ── GET Benchmark ───────────────────────────────────────────────────
    printf("\n=== 1M GET operations (%d threads) ===\n", NUM_THREADS);

    auto get_1  = bench_get<1>(TOTAL_OPS, NUM_THREADS);
    auto get_16 = bench_get<16>(TOTAL_OPS, NUM_THREADS);

    print_result("Store<1>  (1 shard):", get_1);
    print_result("Store<16> (16 shards):", get_16);

    double get_speedup = get_16.ops_per_sec / get_1.ops_per_sec;
    printf("  Throughput speedup: %.2fx\n", get_speedup);

    printf("\n");
    printf("NOTE: p50/p99 include steady_clock::now() overhead (~20ns).\n");
    printf("      Results vary by CPU, cache, and system load.\n");

    return 0;
}
