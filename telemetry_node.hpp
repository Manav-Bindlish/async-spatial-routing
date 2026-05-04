#pragma once

// ════════════════════════════════════════════════════════════════════════════
//  telemetry_node.hpp  —  Async vehicular telemetry ingestion + anomaly detect
//  Design goals:
//    • Lock-free SPSC ring buffer for hot ingestion path
//    • Thread pool (N = std::thread::hardware_concurrency) for routing work
//    • Anomaly detection executes in < 100 ms (benchmarked)
//    • RAII everywhere; no naked new/delete
// ════════════════════════════════════════════════════════════════════════════

#include "graph_engine.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <thread>
#include <vector>

namespace asr {

// ── Telemetry payload (POD, 64-byte aligned for cache line) ──────────────────
struct alignas(64) TelemetryPacket {
    uint32_t  vehicle_id;
    uint16_t  current_node;
    uint16_t  target_node;
    float     speed_kmh;
    float     fuel_pct;
    float     lat;
    float     lon;
    int64_t   timestamp_ns;    // nanoseconds since epoch
    uint8_t   priority;        // 0 = routine, 1 = elevated, 2 = emergency
    uint8_t   flags;
    uint8_t   _pad[10];        // pad to 64 bytes
};
static_assert(sizeof(TelemetryPacket) == 64,
              "TelemetryPacket must be exactly one cache line");

// ── Anomaly types ─────────────────────────────────────────────────────────────
enum class AnomalyKind : uint8_t {
    None           = 0,
    SuddenStop     = 1,   // speed drops > 80 % in < 200 ms
    RouteDeviation = 2,   // vehicle not on computed optimal path
    FuelCritical   = 3,   // fuel_pct < 8 %
    SignalLoss     = 4,   // packet gap > 5 s
    SpeedExcess    = 5,   // speed > zone limit
};

struct AnomalyEvent {
    uint32_t    vehicle_id;
    AnomalyKind kind;
    int64_t     detected_at_ns;
    RouteResult reroute;          // pre-computed alternative route (if any)
};

// ── SPSC ring buffer (lock-free, power-of-2 capacity) ────────────────────────
template <typename T, std::size_t Cap>
class SpscRing {
    static_assert((Cap & (Cap - 1)) == 0, "Cap must be power of 2");
    static constexpr std::size_t kMask = Cap - 1;

    alignas(64) std::atomic<std::size_t> head_{0};   // writer
    alignas(64) std::atomic<std::size_t> tail_{0};   // reader
    std::array<T, Cap>                   buf_{};

public:
    // Producer — returns false if full (non-blocking).
    bool push(const T& item) noexcept {
        const std::size_t h   = head_.load(std::memory_order_relaxed);
        const std::size_t next = (h + 1) & kMask;
        if (next == tail_.load(std::memory_order_acquire)) return false; // full
        buf_[h] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    // Consumer — returns nullopt if empty (non-blocking).
    std::optional<T> pop() noexcept {
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire)) return std::nullopt;
        T item = buf_[t];
        tail_.store((t + 1) & kMask, std::memory_order_release);
        return item;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }
};

// ── Minimal thread pool ───────────────────────────────────────────────────────
//  Uses a work-stealing deque (simplified: single shared queue + condvar)
//  Threads are pinned to cores when POSIX affinity is available.
class ThreadPool {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(std::size_t n_threads = 0);
    ~ThreadPool();

    // Submit a callable; returns immediately. Owns no heap inside hot path.
    void submit(Task t);

    // Drain and wait for all pending tasks.
    void drain() noexcept;

    [[nodiscard]] std::size_t thread_count() const noexcept {
        return workers_.size();
    }

private:
    // Task queue protected by mutex + condvar.
    // Contention is low because tasks are coarse-grained routing jobs.
    struct alignas(64) QueueState {
        std::vector<Task>       tasks;
        std::mutex              mtx;
        std::condition_variable cv;
        std::atomic<std::size_t> pending{0};
        bool                    stop{false};
    };

    std::shared_ptr<QueueState> q_;
    std::vector<std::thread>    workers_;

    void worker_loop(std::shared_ptr<QueueState> q);
};

// ── TelemetryNode ─────────────────────────────────────────────────────────────
//  End-to-end pipeline:
//    ingest() → SPSC ring → thread pool → GraphEngine::find_route() → anomaly
//  Anomaly callback is invoked on a worker thread (not the ingestion thread).
class TelemetryNode {
public:
    static constexpr std::size_t kRingCap = 1024; // must be power-of-2

    using AnomalyCallback = std::function<void(const AnomalyEvent&)>;

    // Construct with a shared (const) graph and optional anomaly callback.
    explicit TelemetryNode(
        std::shared_ptr<const GraphEngine> engine,
        AnomalyCallback                    on_anomaly = {},
        std::size_t                        n_workers  = 0);

    ~TelemetryNode();

    // ── Hot path (called from producer thread) ────────────────────────────
    // Returns false if ring is full (back-pressure signal to producer).
    bool ingest(const TelemetryPacket& pkt) noexcept;

    // Force-process all packets currently in the ring (blocking).
    void flush();

    // Statistics.
    struct Stats {
        uint64_t ingested;
        uint64_t processed;
        uint64_t dropped;          // ring overflow
        uint64_t anomalies;
        int64_t  last_latency_ns;  // most recent processing latency
    };
    [[nodiscard]] Stats stats() const noexcept;

private:
    std::shared_ptr<const GraphEngine> engine_;
    AnomalyCallback                    on_anomaly_;
    ThreadPool                         pool_;

    SpscRing<TelemetryPacket, kRingCap> ring_;

    // Per-vehicle state for anomaly detection (indexed by vehicle_id % kBuckets).
    static constexpr std::size_t kBuckets = 512;
    struct VehicleState {
        float    last_speed{-1.f};
        uint16_t last_node{0};
        int64_t  last_seen_ns{0};
    };
    std::array<VehicleState, kBuckets> vehicle_state_{};

    alignas(64) std::atomic<uint64_t> cnt_ingested_{0};
    alignas(64) std::atomic<uint64_t> cnt_processed_{0};
    alignas(64) std::atomic<uint64_t> cnt_dropped_{0};
    alignas(64) std::atomic<uint64_t> cnt_anomalies_{0};
    alignas(64) std::atomic<int64_t>  last_latency_ns_{0};

    // Background drain thread.
    std::atomic<bool> running_{true};
    std::thread       drain_thread_;
    void drain_loop();

    // Returns detected anomaly kind (or None).
    AnomalyKind classify(const TelemetryPacket& pkt,
                         VehicleState&           vs) const noexcept;
};

} // namespace asr
