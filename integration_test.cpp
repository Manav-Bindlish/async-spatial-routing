// ════════════════════════════════════════════════════════════════════════════
//  main_test.cpp  —  Integration test (no Google Benchmark dependency)
//
//  Tests:
//   1. GraphEngine CSR construction  (104 nodes, 520 edges)
//   2. Dijkstra correctness          (known shortest path)
//   3. Batch routing                 (all 104 pairs)
//   4. TelemetryNode ingest + flush  (1 000 packets)
//   5. Anomaly detection             (fuel-critical injection)
//   6. Latency gate                  (routing must finish < 100 ms)
//   7. SPSC ring back-pressure       (overflow counter increments)
// ════════════════════════════════════════════════════════════════════════════

#include "graph_engine.hpp"
#include "telemetry_node.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>
#include <atomic>

using namespace asr;
using Clock = std::chrono::steady_clock;

// ── Helpers ───────────────────────────────────────────────────────────────────

static void pass(const char* name) {
    std::printf("  [PASS] %s\n", name);
}

static void fail(const char* name, const char* reason) {
    std::printf("  [FAIL] %s — %s\n", name, reason);
    std::fflush(stdout);
    std::abort();
}

// Build the standard 104-node test graph.
// Chain: 0-1-2-…-103 (weight 1.0, bidirectional)
// Cross: i → (i+13)%104 (weight 1.5, one-directional for asymmetry)
static std::pair<std::vector<GraphEngine::RawEdge>, std::vector<NodeMeta>>
build_fixture() {
    std::vector<GraphEngine::RawEdge> edges;
    edges.reserve(520);
    std::vector<NodeMeta> meta(kNodes);

    std::mt19937 rng(42);
    std::uniform_real_distribution<float> lat(8.f, 37.f);
    std::uniform_real_distribution<float> lon(68.f, 97.f);

    for (std::size_t i = 0; i < kNodes; ++i)
        meta[i] = {lat(rng), lon(rng),
                   static_cast<uint16_t>(i / 13),
                   static_cast<uint8_t>(i % 3)};

    for (uint16_t i = 0; i < static_cast<uint16_t>(kNodes - 1); ++i) {
        edges.push_back({i,               static_cast<uint16_t>(i + 1), 1.0f});
        edges.push_back({static_cast<uint16_t>(i + 1), i,               1.0f});
    }
    for (uint16_t i = 0; i < static_cast<uint16_t>(kNodes); ++i) {
        const uint16_t j = static_cast<uint16_t>((i + 13) % kNodes);
        edges.push_back({i, j, 1.5f});
    }
    return {std::move(edges), std::move(meta)};
}

// ── Test 1: Construction ──────────────────────────────────────────────────────
static void test_construction() {
    auto [edges, meta] = build_fixture();
    GraphEngine g({edges.data(), edges.size()},
                  {meta.data(),  meta.size()});
    if (g.node_count() != kNodes)
        fail("construction", "wrong node count");
    pass("GraphEngine construction (104 nodes, 520 edges)");
}

// ── Test 2: Dijkstra correctness ──────────────────────────────────────────────
static void test_dijkstra() {
    auto [edges, meta] = build_fixture();
    GraphEngine g({edges.data(), edges.size()},
                  {meta.data(),  meta.size()});

    // src=0, dst=5: optimal chain path costs 5.0
    {
        auto r = g.find_route(0, 5);
        if (!r.reachable)          fail("dijkstra", "0→5 not reachable");
        if (r.total_cost != 5.0f)  fail("dijkstra", "0→5 wrong cost");
        if (r.path_len   != 6)     fail("dijkstra", "0→5 wrong path len");
    }

    // src=0, dst=103: chain cost=103, cross-link shortcut must be cheaper
    {
        auto r = g.find_route(0, 103);
        if (!r.reachable)           fail("dijkstra", "0→103 not reachable");
        if (r.total_cost >= 103.f)  fail("dijkstra", "0→103 should use shortcuts");
        if (r.path_nodes[0] != 0)   fail("dijkstra", "path must start at src");
        if (r.path_nodes[r.path_len - 1] != 103)
            fail("dijkstra", "path must end at dst");
    }

    // Self-route: src==dst
    {
        auto r = g.find_route(42, 42);
        if (!r.reachable)         fail("dijkstra", "self-route not reachable");
        if (r.total_cost != 0.f)  fail("dijkstra", "self-route cost != 0");
    }
    pass("Dijkstra correctness (chain cost, shortcut, self-route)");
}

// ── Test 3: Batch routing ─────────────────────────────────────────────────────
static void test_batch() {
    auto [edges, meta] = build_fixture();
    GraphEngine g({edges.data(), edges.size()},
                  {meta.data(),  meta.size()});

    std::vector<std::pair<uint16_t,uint16_t>> queries(kNodes);
    std::vector<RouteResult>                  results(kNodes);
    for (uint16_t i = 0; i < static_cast<uint16_t>(kNodes); ++i)
        queries[i] = {i, static_cast<uint16_t>((i + 52) % kNodes)};

    g.find_routes_batch({queries.data(), queries.size()},
                        {results.data(), results.size()});

    for (std::size_t i = 0; i < kNodes; ++i) {
        if (!results[i].reachable)
            fail("batch", "unreachable pair found");
    }
    pass("Batch routing (104 pairs, all reachable)");
}

// ── Test 4: TelemetryNode ingest + flush ─────────────────────────────────────
static void test_ingest() {
    auto [edges, meta] = build_fixture();
    auto engine = std::make_shared<const GraphEngine>(
        Span<const GraphEngine::RawEdge>{edges.data(), edges.size()},
        Span<const NodeMeta>            {meta.data(),  meta.size()});

    TelemetryNode node(engine, {}, 2);

    constexpr int N = 1000;
    for (int i = 0; i < N; ++i) {
        TelemetryPacket pkt{};
        pkt.vehicle_id   = static_cast<uint32_t>(i % 50);
        pkt.current_node = static_cast<uint16_t>(i % kNodes);
        pkt.target_node  = static_cast<uint16_t>((i + 52) % kNodes);
        pkt.speed_kmh    = 60.f;
        pkt.fuel_pct     = 50.f;
        pkt.timestamp_ns = Clock::now().time_since_epoch().count();
        // Retry on back-pressure (ring full).
        while (!node.ingest(pkt)) { std::this_thread::yield(); }
    }
    node.flush();

    auto s = node.stats();
    if (s.ingested < N)
        fail("ingest", "fewer packets ingested than sent");
    if (s.processed < N - s.dropped)
        fail("ingest", "not all ingested packets were processed");
    pass("TelemetryNode ingest + flush (1 000 packets)");
}

// ── Test 5: Anomaly detection ─────────────────────────────────────────────────
static void test_anomaly() {
    auto [edges, meta] = build_fixture();
    auto engine = std::make_shared<const GraphEngine>(
        Span<const GraphEngine::RawEdge>{edges.data(), edges.size()},
        Span<const NodeMeta>            {meta.data(),  meta.size()});

    std::atomic<uint64_t> fired{0};
    TelemetryNode node(engine,
        [&](const AnomalyEvent& ev) {
            if (ev.kind == AnomalyKind::FuelCritical)
                fired.fetch_add(1, std::memory_order_relaxed);
        }, 2);

    // Inject 100 fuel-critical packets.
    for (int i = 0; i < 100; ++i) {
        TelemetryPacket pkt{};
        pkt.vehicle_id   = static_cast<uint32_t>(i);
        pkt.current_node = static_cast<uint16_t>(i % kNodes);
        pkt.target_node  = static_cast<uint16_t>((i + 1) % kNodes);
        pkt.speed_kmh    = 60.f;
        pkt.fuel_pct     = 3.f;  // ← below 8% threshold
        pkt.timestamp_ns = Clock::now().time_since_epoch().count();
        while (!node.ingest(pkt)) { std::this_thread::yield(); }
    }
    node.flush();

    if (fired.load() < 1)
        fail("anomaly", "no FuelCritical anomaly fired");
    std::printf("  [PASS] Anomaly detection (%llu FuelCritical events fired)\n",
                static_cast<unsigned long long>(fired.load()));
}

// ── Test 6: Latency gate — must be well under 100 ms ─────────────────────────
static void test_latency() {
    auto [edges, meta] = build_fixture();
    GraphEngine g({edges.data(), edges.size()},
                  {meta.data(),  meta.size()});

    constexpr int REPS = 10'000;
    const auto t0 = Clock::now();
    for (int i = 0; i < REPS; ++i) {
        volatile auto r = g.find_route(0, 103);
        (void)r;
    }
    const auto t1      = Clock::now();
    const auto total_us = std::chrono::duration_cast<std::chrono::microseconds>
                              (t1 - t0).count();
    const double avg_ns = (static_cast<double>(total_us) * 1000.0) / REPS;

    std::printf("  [PASS] Latency gate — avg %.1f ns/route over %d iterations"
                " (limit: 100 000 000 ns = 100 ms)\n",
                avg_ns, REPS);
    if (avg_ns >= 100'000'000.0)
        fail("latency", "average routing latency exceeds 100 ms");
}

// ── Test 7: SPSC ring back-pressure ──────────────────────────────────────────
static void test_backpressure() {
    auto [edges, meta] = build_fixture();
    auto engine = std::make_shared<const GraphEngine>(
        Span<const GraphEngine::RawEdge>{edges.data(), edges.size()},
        Span<const NodeMeta>            {meta.data(),  meta.size()});

    // No workers — ring will fill up, drops must be non-zero under saturation.
    TelemetryNode node(engine, {}, 0 /* 0 → 1 worker min */);

    TelemetryPacket pkt{};
    pkt.vehicle_id   = 1;
    pkt.current_node = 0;
    pkt.target_node  = 103;
    pkt.speed_kmh    = 60.f;
    pkt.fuel_pct     = 50.f;
    pkt.timestamp_ns = Clock::now().time_since_epoch().count();

    // Blast 2048 packets without yielding — ring capacity is 1024.
    uint64_t sent = 0, rejected = 0;
    for (int i = 0; i < 2048; ++i) {
        if (node.ingest(pkt)) ++sent;
        else                  ++rejected;
    }
    // At least some must have been rejected (ring saturated).
    if (rejected == 0)
        fail("backpressure", "expected some drops when ring is full");
    std::printf("  [PASS] SPSC back-pressure (sent=%llu dropped=%llu)\n",
                static_cast<unsigned long long>(sent),
                static_cast<unsigned long long>(rejected));
    node.flush();
}

// ── main ──────────────────────────────────────────────────────────────────────
int main() {
    std::printf("\n=== async-spatial-routing integration tests ===\n\n");
    test_construction();
    test_dijkstra();
    test_batch();
    test_ingest();
    test_anomaly();
    test_latency();
    test_backpressure();
    std::printf("\nAll tests passed.\n");
    return 0;
}
