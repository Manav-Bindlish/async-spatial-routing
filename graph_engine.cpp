// ════════════════════════════════════════════════════════════════════════════
//  graph_engine.cpp  —  Zero-heap-allocation Dijkstra implementation
//  All hot-path functions are marked [[likely]]/[[unlikely]] where relevant.
//  Thread-local DijkstraScratch gives lock-free concurrency.
// ════════════════════════════════════════════════════════════════════════════

#include "graph_engine.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>

namespace asr {

// ── Construction ──────────────────────────────────────────────────────────────

GraphEngine::GraphEngine(
    std::span<const RawEdge>  edges,
    std::span<const NodeMeta> meta)
{
    if (meta.size() != kNodes) {
        // Only allowed heap allocation is during construction.
        throw std::invalid_argument("meta must contain exactly kNodes entries");
    }
    if (edges.size() > kMaxEdges) {
        throw std::invalid_argument("edge count exceeds kMaxEdges");
    }

    // Copy node metadata.
    for (std::size_t i = 0; i < kNodes; ++i) {
        node_meta_[i] = meta[i];
    }

    // ── Build CSR adjacency list ───────────────────────────────────────────
    // Pass 1: count out-degree per node.
    std::array<uint16_t, kNodes> degree{};
    degree.fill(0);
    for (const auto& e : edges) {
        assert(e.from < kNodes && e.to < kNodes);
        ++degree[e.from];
    }

    // Pass 2: exclusive prefix-sum → row pointers.
    adj_start_[0] = 0;
    for (std::size_t n = 0; n < kNodes; ++n) {
        adj_start_[n + 1] = static_cast<uint16_t>(adj_start_[n] + degree[n]);
    }
    edge_count_ = adj_start_[kNodes];

    // Pass 3: fill edge array (use degree[] as write cursor).
    degree.fill(0);
    for (const auto& e : edges) {
        const uint16_t slot = adj_start_[e.from] + degree[e.from];
        adj_edges_[slot]    = {e.to, e.weight};
        ++degree[e.from];
    }
}

// ── Accessors ─────────────────────────────────────────────────────────────────

std::span<const Edge> GraphEngine::edges_of(uint16_t node) const noexcept {
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
    // Each thread gets its own scratch → zero contention, zero allocation in
    // subsequent calls.
    thread_local DijkstraScratch scratch;
    return scratch;
}

void GraphEngine::DijkstraScratch::reset() noexcept {
    dist.fill(kInfWeight);
    prev.fill(static_cast<uint16_t>(kNodes)); // sentinel = "no predecessor"
    visited.fill(false);
    heap_size = 0;
}

// ── Binary min-heap helpers ───────────────────────────────────────────────────
// Key   = dist[node]
// Heap  = array of node IDs ordered by dist
// pos_in_heap[node] = current position of node in heap[]

void GraphEngine::heap_sift_up(DijkstraScratch& s, uint16_t pos) noexcept {
    while (pos > 0) {
        const uint16_t parent = (pos - 1) / 2;
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
        if (l < s.heap_size && s.dist[s.heap[l]] < s.dist[s.heap[smallest]])
            smallest = l;
        if (r < s.heap_size && s.dist[s.heap[r]] < s.dist[s.heap[smallest]])
            smallest = r;
        if (smallest == pos) break;
        std::swap(s.heap[smallest], s.heap[pos]);
        s.pos_in_heap[s.heap[smallest]] = smallest;
        s.pos_in_heap[s.heap[pos]]      = pos;
        pos = smallest;
    }
}

void GraphEngine::heap_push(DijkstraScratch& s, uint16_t node) noexcept {
    const uint16_t pos = s.heap_size++;
    s.heap[pos]          = node;
    s.pos_in_heap[node]  = pos;
    heap_sift_up(s, pos);
}

uint16_t GraphEngine::heap_pop(DijkstraScratch& s) noexcept {
    const uint16_t top = s.heap[0];
    const uint16_t last = --s.heap_size;
    s.heap[0]            = s.heap[last];
    s.pos_in_heap[s.heap[0]] = 0;
    if (s.heap_size > 0) heap_sift_down(s, 0);
    return top;
}

void GraphEngine::heap_decrease_key(DijkstraScratch& s, uint16_t node) noexcept {
    heap_sift_up(s, s.pos_in_heap[node]);
}

// ── Core hot path: find_route ─────────────────────────────────────────────────
// NO heap allocation. All state in thread-local DijkstraScratch.

RouteResult GraphEngine::find_route(uint16_t src, uint16_t dst) const noexcept {
    assert(src < kNodes && dst < kNodes);

    DijkstraScratch& s = thread_scratch();
    s.reset();

    s.dist[src] = 0.0f;
    heap_push(s, src);

    while (s.heap_size > 0) {
        const uint16_t u = heap_pop(s);

        if (u == dst) [[likely]] break;       // early exit
        if (s.visited[u]) continue;
        s.visited[u] = true;

        for (const Edge& e : edges_of(u)) {
            if (s.visited[e.to]) [[likely]] continue;
            const float alt = s.dist[u] + e.weight;
            if (alt < s.dist[e.to]) {
                s.dist[e.to] = alt;
                s.prev[e.to] = u;
                if (s.pos_in_heap[e.to] < s.heap_size &&
                    s.heap[s.pos_in_heap[e.to]] == e.to)
                {
                    heap_decrease_key(s, e.to);
                } else {
                    heap_push(s, e.to);
                }
            }
        }
    }

    return reconstruct_path(s, src, dst);
}

RouteResult GraphEngine::reconstruct_path(
    const DijkstraScratch& s,
    uint16_t src,
    uint16_t dst) const noexcept
{
    RouteResult result{};
    result.total_cost = s.dist[dst];
    result.reachable  = (s.dist[dst] < kInfWeight);
    result.path_len   = 0;

    if (!result.reachable) return result;

    // Walk back from dst to src along prev[] chain.
    // Temporary path stored in result.path_nodes from the end.
    uint8_t  len  = 0;
    uint16_t cur  = dst;
    while (cur != src && len < static_cast<uint8_t>(kNodes)) {
        result.path_nodes[len++] = cur;
        cur = s.prev[cur];
    }
    result.path_nodes[len++] = src;
    result.path_len = len;

    // Reverse in-place.
    std::reverse(result.path_nodes.begin(),
                 result.path_nodes.begin() + len);
    return result;
}

// ── Batch routing ─────────────────────────────────────────────────────────────

void GraphEngine::find_routes_batch(
    std::span<const std::pair<uint16_t,uint16_t>> queries,
    std::span<RouteResult>                         results) const noexcept
{
    assert(results.size() >= queries.size());
    for (std::size_t i = 0; i < queries.size(); ++i) {
        results[i] = find_route(queries[i].first, queries[i].second);
    }
}

} // namespace asr
