#pragma once

// ════════════════════════════════════════════════════════════════════════════
//  telemetry_node.hpp  —  Async vehicular telemetry ingestion + anomaly detect
//  C++17 compliant: all includes explicit, Span<T> from graph_engine.hpp
// ════════════════════════════════════════════════════════════════════════════

#include "graph_engine.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace asr {

// ── Telemetry payload — 64-byte / one cache line ──────────────────────────────
struct alignas(64) TelemetryPacket {
    uint32_t vehicle_id;
    uint16_t current_node;
    uint16_t target_node;
    float    speed_kmh;
    float    fuel_pct;
    float    lat;
    float    lon;
    int64_t  timestamp_ns;
    uint8_t  priority;   // 0=routine, 1=elevated, 2=emergency
    uint8_t  flags;
    uint8_t  _pad[10];
};
static_assert(sizeof(TelemetryPacket) == 64,
              "TelemetryPacket must be exactly one cache line");

// ── Anomaly types ─────────────────────────────────────────────────────────────
enum class AnomalyKind : uint8_t {
    None           = 0,
    SuddenStop     = 1,
    RouteDeviation = 2,
    FuelCritical   = 3,
    SignalLoss     = 4,
    SpeedExcess    = 5,
};

struct AnomalyEvent {
    uint32_t    vehicle_id;
    AnomalyKind kind;
    int64_t     detected_at_ns;
    RouteResult reroute;
};

// ── Lock-free SPSC ring buffer ────────────────────────────────────────────────
template <typename T, std::size_t Cap>
class SpscRing {
    static_assert((Cap & (Cap - 1)) == 0, "Cap must be a power of 2");
    static constexpr std::size_t kMask = Cap - 1;

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
    std::array<T, Cap> buf_{};

public:
    bool push(const T& item) noexcept {
        const std::size_t h    = head_.load(std::memory_order_relaxed);
        const std::size_t next = (h + 1) & kMask;
        if (next == tail_.load(std::memory_order_acquire)) return false;
        buf_[h] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    std::optional<T> pop() noexcept {
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire)) return std::nullopt;
        T item = buf_[t];
        tail_.store((t + 1) & kMask, std::memory_order_release);
        return item;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }
};

// ── Thread pool ───────────────────────────────────────────────────────────────
class ThreadPool {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(std::size_t n_threads = 0);
    ~ThreadPool();

    void submit(Task t);
    void drain() noexcept;

    std::size_t thread_count() const noexcept { return workers_.size(); }

private:
    struct QueueState {
        std::vector<Task>        tasks;
        std::mutex               mtx;
        std::condition_variable  cv;
        std::atomic<std::size_t> pending{0};
        bool                     stop{false};
    };

    std::shared_ptr<QueueState> q_;
    std::vector<std::thread>    workers_;

    void worker_loop(std::shared_ptr<QueueState> q);
};

// ── TelemetryNode ─────────────────────────────────────────────────────────────
class TelemetryNode {
public:
    static constexpr std::size_t kRingCap = 1024; // power-of-2

    using AnomalyCallback = std::function<void(const AnomalyEvent&)>;

    explicit TelemetryNode(std::shared_ptr<const GraphEngine> engine,
                            AnomalyCallback                    on_anomaly = {},
                            std::size_t                        n_workers  = 0);
    ~TelemetryNode();

    bool ingest(const TelemetryPacket& pkt) noexcept;
    void flush();

    struct Stats {
        uint64_t ingested;
        uint64_t processed;
        uint64_t dropped;
        uint64_t anomalies;
        int64_t  last_latency_ns;
    };
    Stats stats() const noexcept;

private:
    std::shared_ptr<const GraphEngine>  engine_;
    AnomalyCallback                     on_anomaly_;
    ThreadPool                          pool_;

    SpscRing<TelemetryPacket, kRingCap> ring_;

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

    std::atomic<bool> running_{true};
    std::thread       drain_thread_;

    void       drain_loop();
    AnomalyKind classify(const TelemetryPacket& pkt,
                          VehicleState&           vs) const noexcept;
};

} // namespace asr
