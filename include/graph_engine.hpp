#pragma once

// ════════════════════════════════════════════════════════════════════════════
//  graph_engine.hpp  —  Cache-friendly, zero-heap-allocation Dijkstra engine
//  Domain  : NITI Aayog real-time vehicular telemetry routing
//  Grid    : 104-node distributed graph
//  Standard: C++17  (Span<T> shim replaces C++20 std::span)
// ════════════════════════════════════════════════════════════════════════════

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace asr {

// ── Lightweight non-owning view (C++17 replacement for std::span) ─────────────
template <typename T>
struct Span {
    T*          data_;
    std::size_t size_;

    constexpr Span(T* d, std::size_t s) noexcept : data_(d), size_(s) {}

    // Implicit construction from vector (removes const for pointer type).
    Span(std::vector<std::remove_cv_t<T>>& v) noexcept          // NOLINT
        : data_(v.data()), size_(v.size()) {}
    Span(const std::vector<std::remove_cv_t<T>>& v) noexcept    // NOLINT
        : data_(v.data()), size_(v.size()) {}

    constexpr T*          begin()  const noexcept { return data_; }
    constexpr T*          end()    const noexcept { return data_ + size_; }
    constexpr std::size_t size()   const noexcept { return size_; }
    constexpr bool        empty()  const noexcept { return size_ == 0; }
    constexpr T& operator[](std::size_t i) const noexcept { return data_[i]; }
};

// ── Compile-time topology constants ──────────────────────────────────────────
inline constexpr std::size_t kNodes     = 104;
inline constexpr std::size_t kMaxEdges  = 520;
inline constexpr float       kInfWeight = std::numeric_limits<float>::infinity();

// ── POD types ─────────────────────────────────────────────────────────────────
struct Edge {
    uint16_t to;
    float    weight;
};

struct NodeMeta {
    float    latitude;
    float    longitude;
    uint16_t zone_id;
    uint8_t  tier;
};

struct RouteResult {
    float                        total_cost;
    std::array<uint16_t, kNodes> path_nodes;
    uint8_t                      path_len;
    bool                         reachable;
};

// ════════════════════════════════════════════════════════════════════════════
//  GraphEngine — zero-allocation Dijkstra via thread_local scratch
// ════════════════════════════════════════════════════════════════════════════
class GraphEngine {
public:
    struct RawEdge { uint16_t from, to; float weight; };

    explicit GraphEngine(Span<const RawEdge>  edges,
                         Span<const NodeMeta> meta);

    [[nodiscard]] RouteResult find_route(uint16_t src,
                                         uint16_t dst) const noexcept;

    void find_routes_batch(
        Span<const std::pair<uint16_t, uint16_t>> queries,
        Span<RouteResult>                          results) const noexcept;

    [[nodiscard]] Span<const Edge> edges_of(uint16_t node) const noexcept;
    [[nodiscard]] const NodeMeta&  meta_of (uint16_t node) const noexcept;
    [[nodiscard]] std::size_t      node_count()            const noexcept
    { return kNodes; }

private:
    std::array<uint16_t, kNodes + 1> adj_start_{};
    std::array<Edge,     kMaxEdges>  adj_edges_{};
    std::size_t                      edge_count_{0};
    std::array<NodeMeta, kNodes>     node_meta_{};

    struct alignas(64) DijkstraScratch {
        std::array<float,    kNodes> dist;
        std::array<uint16_t, kNodes> prev;
        std::array<bool,     kNodes> visited;
        std::array<uint16_t, kNodes> heap;
        std::array<uint16_t, kNodes> pos_in_heap;
        uint16_t                     heap_size{0};
        void reset() noexcept;
    };

    [[nodiscard]] static DijkstraScratch& thread_scratch() noexcept;

    static void     heap_push        (DijkstraScratch& s, uint16_t node) noexcept;
    static uint16_t heap_pop         (DijkstraScratch& s)                noexcept;
    static void     heap_decrease_key(DijkstraScratch& s, uint16_t node) noexcept;
    static void     heap_sift_up     (DijkstraScratch& s, uint16_t pos)  noexcept;
    static void     heap_sift_down   (DijkstraScratch& s, uint16_t pos)  noexcept;

    RouteResult reconstruct_path(const DijkstraScratch& s,
                                  uint16_t src,
                                  uint16_t dst) const noexcept;
};

} // namespace asr
