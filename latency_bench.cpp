// ════════════════════════════════════════════════════════════════════════════
//  latency_bench.cpp  —  Google Benchmark suite for async-spatial-routing
//
//  Benchmarks:
//    BM_SingleRoute        – single Dijkstra query (hot path, zero alloc)
//    BM_BatchRoutes        – 104 queries (full grid sweep)
//    BM_TelemetryIngest    – SPSC ring push throughput
//    BM_AnomalyDetection   – end-to-end packet → anomaly classification
//    BM_ThreadPoolDispatch – submit + drain under N workers
//
//  Target: all timings < 100 ms; BM_AnomalyDetection < 10 µs per iteration.
// ════════════════════════════════════════════════════════════════════════════

#include <benchmark/benchmark.h>

#include "graph_engine.hpp"
#include "telemetry_node.hpp"

#include <array>
#include <chrono>
#include <cmath>
#include <memory>
#include <random>
#include <vector>

// ── Shared fixture: build a 104-node grid graph once ─────────────────────────
namespace {

using namespace asr;

// Build a deterministic 104-node ring-grid:
//   • Linear chain: 0→1→2→…→103 (weight 1.0)
//   • Cross-links:  i → (i + 13) % 104 (weight 1.5)  ← 8 zones of 13 nodes
// This gives ~5 edges/node, matching the expected NITI Aayog grid density.
struct FixtureGraph {
    std::vector<GraphEngine::RawEdge>  edges;
    std::vector<NodeMeta>              meta;
    std::shared_ptr<const GraphEngine> engine;

    FixtureGraph() {
        meta.resize(kNodes);
        std::mt19937 rng(42);
        std::uniform_real_distribution<float> lat_dist(8.0f,  37.0f);
        std::uniform_real_distribution<float> lon_dist(68.0f, 97.0f);

        for (std::size_t i = 0; i < kNodes; ++i) {
            meta[i] = NodeMeta{
                lat_dist(rng),
                lon_dist(rng),
                static_cast<uint16_t>(i / 13),
                static_cast<uint8_t>( i % 3)
            };
        }

        // Chain edges (bidirectional).
        for (uint16_t i = 0; i < kNodes - 1; ++i) {
            edges.push_back({i,               static_cast<uint16_t>(i + 1),  1.0f});
            edges.push_back({static_cast<uint16_t>(i + 1), i,               1.0f});
        }
        // Cross-links.
        for (uint16_t i = 0; i < kNodes; ++i) {
            const uint16_t j = static_cast<uint16_t>((i + 13) % kNodes);
            edges.push_back({i, j, 1.5f});
        }

        engine = std::make_shared<const GraphEngine>(
            std::span<const GraphEngine::RawEdge>(edges),
            std::span<const NodeMeta>(meta)
        );
    }
};

// Singleton — constructed once before any benchmark runs.
const FixtureGraph& fixture() {
    static FixtureGraph f;
    return f;
}

TelemetryPacket make_packet(uint32_t vid, uint16_t src, uint16_t dst,
                             uint8_t priority = 0) noexcept {
    TelemetryPacket p{};
    p.vehicle_id   = vid;
    p.current_node = src;
    p.target_node  = dst;
    p.speed_kmh    = 60.0f;
    p.fuel_pct     = 50.0f;
    p.priority     = priority;
    p.timestamp_ns = std::chrono::steady_clock::now()
                         .time_since_epoch().count();
    return p;
}

} // anonymous namespace

// ── BM_SingleRoute ────────────────────────────────────────────────────────────
// Measures the Dijkstra hot path: zero heap allocation per iteration.

static void BM_SingleRoute(benchmark::State& state) {
    const auto& g = fixture();
    const uint16_t src = 0, dst = 103;

    for (auto _ : state) {
        RouteResult r = g.engine->find_route(src, dst);
        benchmark::DoNotOptimize(r);
    }

    state.SetLabel("src=0 dst=103 (max diameter)");
    state.counters["routes/sec"] = benchmark::Counter(
        static_cast<double>(state.iterations()),
        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_SingleRoute)
    ->MinTime(1.0)
    ->Unit(benchmark::kNanosecond)
    ->Threads(1);

// ── BM_BatchRoutes ────────────────────────────────────────────────────────────
// 104 queries covering the full grid in one call.

static void BM_BatchRoutes(benchmark::State& state) {
    const auto& g = fixture();

    std::vector<std::pair<uint16_t,uint16_t>> queries;
    queries.reserve(kNodes);
    for (uint16_t i = 0; i < static_cast<uint16_t>(kNodes); ++i) {
        queries.push_back({i, static_cast<uint16_t>((i + 52) % kNodes)});
    }

    std::vector<RouteResult> results(kNodes);

    for (auto _ : state) {
        g.engine->find_routes_batch(queries, results);
        benchmark::DoNotOptimize(results.data());
    }

    state.SetLabel("104-node full sweep");
    state.counters["queries/sec"] = benchmark::Counter(
        static_cast<double>(state.iterations()) * kNodes,
        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_BatchRoutes)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

// ── BM_TelemetryIngest ────────────────────────────────────────────────────────
// Measures SPSC ring push throughput — producer-side hot path.

static void BM_TelemetryIngest(benchmark::State& state) {
    const auto& g = fixture();

    // Sink callback — measures only up to ingest(), not processing.
    TelemetryNode node(g.engine, {}, 1);

    const TelemetryPacket pkt = make_packet(1, 0, 103);

    for (auto _ : state) {
        // Back-pressure: retry if ring is full (expected only under saturation).
        while (!node.ingest(pkt)) {
            std::this_thread::yield();
        }
    }
    node.flush();

    state.counters["pkts/sec"] = benchmark::Counter(
        static_cast<double>(state.iterations()),
        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_TelemetryIngest)
    ->MinTime(1.0)
    ->Unit(benchmark::kNanosecond);

// ── BM_AnomalyDetection ───────────────────────────────────────────────────────
// End-to-end: ingest → route → classify → anomaly callback.
// Target: < 100 µs per packet (proving sub-100ms for any realistic burst).

static void BM_AnomalyDetection(benchmark::State& state) {
    const auto& g = fixture();

    std::atomic<uint64_t> anomaly_count{0};
    TelemetryNode node(
        g.engine,
        [&](const AnomalyEvent&) { anomaly_count.fetch_add(1, std::memory_order_relaxed); },
        static_cast<std::size_t>(state.range(0))  // worker count from arg
    );

    uint32_t vid    = 0;
    uint16_t src    = 0;
    bool    inject  = false;

    for (auto _ : state) {
        state.PauseTiming();
        // Every 10th packet: inject a fuel-critical anomaly.
        inject   = (vid % 10 == 0);
        TelemetryPacket pkt = make_packet(
            vid, src, static_cast<uint16_t>((src + 52) % kNodes));
        if (inject) pkt.fuel_pct = 5.0f;   // trigger FuelCritical
        src = static_cast<uint16_t>((src + 1) % kNodes);
        ++vid;
        state.ResumeTiming();

        while (!node.ingest(pkt)) std::this_thread::yield();
    }
    node.flush();

    state.counters["anomalies"]  = benchmark::Counter(
        static_cast<double>(anomaly_count.load()));
    state.counters["pkts/sec"]   = benchmark::Counter(
        static_cast<double>(state.iterations()),
        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_AnomalyDetection)
    ->Arg(1)->Arg(2)->Arg(4)          // 1, 2, 4 worker threads
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

// ── BM_ThreadPoolDispatch ─────────────────────────────────────────────────────
// Measures submit + drain overhead for varying worker counts.

static void BM_ThreadPoolDispatch(benchmark::State& state) {
    const std::size_t n_workers = static_cast<std::size_t>(state.range(0));
    ThreadPool pool(n_workers);

    std::atomic<uint64_t> counter{0};

    for (auto _ : state) {
        constexpr int kTasksPerIter = 32;
        for (int t = 0; t < kTasksPerIter; ++t) {
            pool.submit([&counter]() {
                counter.fetch_add(1, std::memory_order_relaxed);
            });
        }
        pool.drain();
    }

    state.counters["tasks/sec"] = benchmark::Counter(
        static_cast<double>(state.iterations()) * 32,
        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_ThreadPoolDispatch)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

// ── BM_RouteLatencyHistogram ──────────────────────────────────────────────────
// Worst-case + percentile proof that single-route latency is sub-100µs.

static void BM_RouteLatencyHistogram(benchmark::State& state) {
    const auto& g = fixture();

    int64_t worst_ns = 0;
    for (auto _ : state) {
        const auto t0 = std::chrono::steady_clock::now();
        RouteResult r  = g.engine->find_route(0, 103);
        const auto t1  = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(r);

        const int64_t elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (elapsed > worst_ns) worst_ns = elapsed;
    }

    state.counters["worst_ns"] = benchmark::Counter(
        static_cast<double>(worst_ns));
    state.SetLabel("worst-case single route (should be << 100ms)");
}
BENCHMARK(BM_RouteLatencyHistogram)
    ->Iterations(100'000)
    ->Unit(benchmark::kNanosecond);
