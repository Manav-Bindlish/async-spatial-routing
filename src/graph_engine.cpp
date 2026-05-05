// ════════════════════════════════════════════════════════════════════════════
//  graph_engine.cpp  —  Zero-heap-allocation Dijkstra (C++17)
// ════════════════════════════════════════════════════════════════════════════

#include "graph_engine.hpp"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace asr {

// ── Constructor ───────────────────────────────────────────────────────────────

GraphEngine::GraphEngine(Span<const RawEdge>  edges,
                         Span<const NodeMeta> meta)
{
    if (meta.size() != kNodes)
        throw std::invalid_argument("meta must contain exactly kNodes entries");
    if (edges.size() > kMaxEdges)
        throw std::invalid_argument("edge count exceeds kMaxEdges");

    for (std::size_t i = 0; i < kNodes; ++i)
        node_meta_[i] = meta[i];

    // Pass 1: count out-degree.
    std::array<uint16_t, kNodes> degree{};
    degree.fill(0);
    for (std::size_t i = 0; i < edges.size(); ++i) {
        assert(edges[i].from < kNodes && edges[i].to < kNodes);
        ++degree[edges[i].from];
    }

    // Pass 2: exclusive prefix-sum → CSR row pointers.
    adj_start_[0] = 0;
    for (std::size_t n = 0; n < kNodes; ++n)
        adj_start_[n + 1] = static_cast<uint16_t>(adj_start_[n] + degree[n]);
    edge_count_ = adj_start_[kNodes];

    // Pass 3: scatter edges into CSR columns.
    degree.fill(0);
    for (std::size_t i = 0; i < edges.size(); ++i) {
        const auto& e   = edges[i];
        const uint16_t slot = adj_start_[e.from] + degree[e.from];
        adj_edges_[slot]    = {e.to, e.weight};
        ++degree[e.from];
    }
}

// ── Accessors ─────────────────────────────────────────────────────────────────

Span<const Edge> GraphEngine::edges_of(uint16_t node) const noexcept {
    assert(node < kNodes);
    const uint16_t start = adj_start_[node];
    const uint16_t end   = adj_start_[node + 1];
    return {adj_edges_.data() + start,
            static_cast<std::size_t>(end - start)};
}

const NodeMeta& GraphEngine::meta_of(uint16_t node) const noexcept {
    assert(node < kNodes);
    return node_meta_[node];
}

// ── Thread-local scratch ──────────────────────────────────────────────────────

GraphEngine::DijkstraScratch& GraphEngine::thread_scratch() noexcept {
    thread_local DijkstraScratch scratch;
    return scratch;
}

void GraphEngine::DijkstraScratch::reset() noexcept {
    dist.fill(kInfWeight);
    prev.fill(static_cast<uint16_t>(kNodes)); // sentinel
    visited.fill(false);
    heap_size = 0;
}

// ── Binary min-heap helpers ───────────────────────────────────────────────────

void GraphEngine::heap_sift_up(DijkstraScratch& s, uint16_t pos) noexcept {
    while (pos > 0) {
        const uint16_t parent = static_cast<uint16_t>((pos - 1) / 2);
        if (s.dist[s.heap[parent]] <= s.dist[s.heap[pos]]) break;
        std::swap(s.heap[parent], s.heap[pos]);
        s.pos_in_heap[s.heap[parent]] = parent;
        s.pos_in_heap[s.heap[pos]]    = pos;
        pos = parent;
    }
}

void GraphEngine::heap_sift_down(DijkstraScratch& s, uint16_t pos) noexcept {
    for (;;) {
        uint16_t smallest = pos;
        const uint16_t l  = static_cast<uint16_t>(2 * pos + 1);
        const uint16_t r  = static_cast<uint16_t>(2 * pos + 2);
        if (l < s.heap_size &&
            s.dist[s.heap[l]] < s.dist[s.heap[smallest]]) smallest = l;
        if (r < s.heap_size &&
            s.dist[s.heap[r]] < s.dist[s.heap[smallest]]) smallest = r;
        if (smallest == pos) break;
        std::swap(s.heap[smallest], s.heap[pos]);
        s.pos_in_heap[s.heap[smallest]] = smallest;
        s.pos_in_heap[s.heap[pos]]      = pos;
        pos = smallest;
    }
}

void GraphEngine::heap_push(DijkstraScratch& s, uint16_t node) noexcept {
    const uint16_t pos   = s.heap_size++;
    s.heap[pos]          = node;
    s.pos_in_heap[node]  = pos;
    heap_sift_up(s, pos);
}

uint16_t GraphEngine::heap_pop(DijkstraScratch& s) noexcept {
    const uint16_t top  = s.heap[0];
    const uint16_t last = --s.heap_size;
    s.heap[0]                = s.heap[last];
    s.pos_in_heap[s.heap[0]] = 0;
    if (s.heap_size > 0) heap_sift_down(s, 0);
    return top;
}

void GraphEngine::heap_decrease_key(DijkstraScratch& s,
                                     uint16_t node) noexcept {
    heap_sift_up(s, s.pos_in_heap[node]);
}

// ── Hot path: find_route ──────────────────────────────────────────────────────

RouteResult GraphEngine::find_route(uint16_t src,
                                     uint16_t dst) const noexcept {
    assert(src < kNodes && dst < kNodes);
    DijkstraScratch& s = thread_scratch();
    s.reset();

    s.dist[src] = 0.0f;
    heap_push(s, src);

    while (s.heap_size > 0) {
        const uint16_t u = heap_pop(s);
        if (u == dst) break;
        if (s.visited[u]) continue;
        s.visited[u] = true;

        const Span<const Edge> nbrs = edges_of(u);
        for (std::size_t ei = 0; ei < nbrs.size(); ++ei) {
            const Edge& e = nbrs[ei];
            if (s.visited[e.to]) continue;
            const float alt = s.dist[u] + e.weight;
            if (alt < s.dist[e.to]) {
                s.dist[e.to] = alt;
                s.prev[e.to] = u;
                if (s.pos_in_heap[e.to] < s.heap_size &&
                    s.heap[s.pos_in_heap[e.to]] == e.to) {
                    heap_decrease_key(s, e.to);
                } else {
                    heap_push(s, e.to);
                }
            }
        }
    }
    return reconstruct_path(s, src, dst);
}

RouteResult GraphEngine::reconstruct_path(const DijkstraScratch& s,
                                           uint16_t src,
                                           uint16_t dst) const noexcept {
    RouteResult result{};
    result.total_cost = s.dist[dst];
    result.reachable  = (s.dist[dst] < kInfWeight);
    result.path_len   = 0;
    if (!result.reachable) return result;

    uint8_t  len = 0;
    uint16_t cur = dst;
    while (cur != src && len < static_cast<uint8_t>(kNodes)) {
        result.path_nodes[len++] = cur;
        cur = s.prev[cur];
    }
    result.path_nodes[len++] = src;
    result.path_len = len;
    std::reverse(result.path_nodes.begin(),
                 result.path_nodes.begin() + len);
    return result;
}

// ── Batch routing ─────────────────────────────────────────────────────────────

void GraphEngine::find_routes_batch(
    Span<const std::pair<uint16_t, uint16_t>> queries,
    Span<RouteResult>                          results) const noexcept
{
    assert(results.size() >= queries.size());
    for (std::size_t i = 0; i < queries.size(); ++i)
        results[i] = find_route(queries[i].first, queries[i].second);
}

} // namespace asr
