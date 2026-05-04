#pragma once

// ════════════════════════════════════════════════════════════════════════════
//  graph_engine.hpp  —  Cache-friendly, zero-heap-allocation Dijkstra engine
//  Domain  : NITI Aayog real-time vehicular telemetry routing
//  Grid    : 104-node distributed graph
//  C++17   : std::array, std::span, structured bindings
// ════════════════════════════════════════════════════════════════════════════

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <vector>

namespace asr {

// ── Compile-time topology constants ──────────────────────────────────────────
inline constexpr std::size_t kNodes      = 104;
inline constexpr std::size_t kMaxEdges   = 520;   // ~5 edges/node average
inline constexpr float       kInfWeight  = std::numeric_limits<float>::infinity();

// ── POD types (trivially copyable → cache-line friendly) ─────────────────────
struct Edge {
    uint16_t to;       // destination node id  (0..103)
    float    weight;   // kilometres or latency ms
};

struct NodeMeta {
    float    latitude;
    float    longitude;
    uint16_t zone_id;  // administrative zone (NITI Aayog grid sector)
    uint8_t  tier;     // 0 = hub, 1 = relay, 2 = leaf
};

// ── Result of a shortest-path query ──────────────────────────────────────────
struct RouteResult {
    float                           total_cost;
    std::array<uint16_t, kNodes>    path_nodes;   // node IDs in order
    uint8_t                         path_len;
    bool                            reachable;
};

// ════════════════════════════════════════════════════════════════════════════
//  GraphEngine
//
//  All hot-path state lives in std::array (stack / pre-allocated heap).
//  NO new/malloc inside find_route() or relax_edges().
// ════════════════════════════════════════════════════════════════════════════
class GraphEngine {
public:
    // Construction builds adjacency list from edge list.
    // edges_flat: contiguous {from, to, weight} triples flattened to a span.
    struct RawEdge { uint16_t from, to; float weight; };

    explicit GraphEngine(
        std::span<const RawEdge>   edges,
        std::span<const NodeMeta>  meta    // must be kNodes elements
    );

    // ── Hot path ─────────────────────────────────────────────────────────────
    // find_route: Dijkstra with binary-min-heap over pre-allocated scratch.
    // ZERO dynamic allocation. Reentrant if called from separate threads with
    // different RouteContext scratch buffers.
    [[nodiscard]] RouteResult find_route(uint16_t src, uint16_t dst) const noexcept;

    // Batch: process multiple (src, dst) pairs, writing into results[].
    // Caller owns the output span; no allocation inside.
    void find_routes_batch(
        std::span<const std::pair<uint16_t,uint16_t>> queries,
        std::span<RouteResult>                         results
    ) const noexcept;

    // ── Inspection ───────────────────────────────────────────────────────────
    [[nodiscard]] std::span<const Edge>     edges_of(uint16_t node) const noexcept;
    [[nodiscard]] const NodeMeta&           meta_of(uint16_t node)  const noexcept;
    [[nodiscard]] std::size_t               node_count()            const noexcept { return kNodes; }

private:
    // ── Adjacency list stored contiguously ───────────────────────────────────
    // adj_edges_[adj_start_[n] .. adj_start_[n+1]) = edges from node n
    std::array<uint16_t, kNodes + 1>   adj_start_{};   // CSR row pointers
    std::array<Edge,     kMaxEdges>    adj_edges_{};   // CSR column data
    std::size_t                        edge_count_{0};

    // ── Node metadata ────────────────────────────────────────────────────────
    std::array<NodeMeta, kNodes>       node_meta_{};

    // ── Per-query scratch (allocated once, reused) ───────────────────────────
    // Dijkstra internal state — NOT shared; callers own their scratch.
    struct alignas(64) DijkstraScratch {
        std::array<float,    kNodes> dist;
        std::array<uint16_t, kNodes> prev;
        std::array<bool,     kNodes> visited;

        // Intrusive binary min-heap  (index = node, key = dist[node])
        std::array<uint16_t, kNodes> heap;
        std::array<uint16_t, kNodes> pos_in_heap;   // heap position of node i
        uint16_t                     heap_size{0};

        void reset() noexcept;
    };

    // Thread-local scratch eliminates mutex in the hot path.
    [[nodiscard]] static DijkstraScratch& thread_scratch() noexcept;

    // Internal helpers — all noexcept, no allocation.
    static void   heap_push(DijkstraScratch& s, uint16_t node)   noexcept;
    static uint16_t heap_pop(DijkstraScratch& s)                 noexcept;
    static void   heap_decrease_key(DijkstraScratch& s,
                                    uint16_t node)               noexcept;
    static void   heap_sift_up(DijkstraScratch& s,
                               uint16_t pos)                     noexcept;
    static void   heap_sift_down(DijkstraScratch& s,
                                 uint16_t pos)                   noexcept;

    RouteResult reconstruct_path(
        const DijkstraScratch& s,
        uint16_t src,
        uint16_t dst
    ) const noexcept;
};

} // namespace asr
