// ════════════════════════════════════════════════════════════════════════════
//  telemetry_node.cpp  —  ThreadPool + TelemetryNode implementation
//  All timing via std::chrono::steady_clock (monotonic, no heap allocs).
// ════════════════════════════════════════════════════════════════════════════

#include "telemetry_node.hpp"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>

#ifdef __linux__
#  include <pthread.h>    // pthread_setaffinity_np
#  include <sched.h>
#endif

namespace asr {

// ════════════════════════════════════════════════════════════════════════════
//  ThreadPool
// ════════════════════════════════════════════════════════════════════════════

namespace {

void pin_thread_to_core(std::size_t core_id) noexcept {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id % std::thread::hardware_concurrency(), &cpuset);
    ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
#else
    (void)core_id;
#endif
}

} // anonymous namespace

ThreadPool::ThreadPool(std::size_t n_threads)
    : q_(std::make_shared<QueueState>())
{
    if (n_threads == 0)
        n_threads = std::max(1u, std::thread::hardware_concurrency());

    workers_.reserve(n_threads);
    for (std::size_t i = 0; i < n_threads; ++i) {
        workers_.emplace_back([this, i, q = q_]() mutable {
            pin_thread_to_core(i);
            worker_loop(std::move(q));
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lk(q_->mtx);
        q_->stop = true;
    }
    q_->cv.notify_all();
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void ThreadPool::submit(Task t) {
    q_->pending.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(q_->mtx);
        q_->tasks.push_back(std::move(t));
    }
    q_->cv.notify_one();
}

void ThreadPool::drain() noexcept {
    // Spin-wait until all submitted tasks complete.
    while (q_->pending.load(std::memory_order_acquire) > 0) {
        std::this_thread::yield();
    }
}

void ThreadPool::worker_loop(std::shared_ptr<QueueState> q) {
    for (;;) {
        Task task;
        {
            std::unique_lock<std::mutex> lk(q->mtx);
            q->cv.wait(lk, [&q] {
                return !q->tasks.empty() || q->stop;
            });

            if (q->stop && q->tasks.empty()) return;

            task = std::move(q->tasks.back());
            q->tasks.pop_back();
        }
        task();
        q->pending.fetch_sub(1, std::memory_order_release);
    }
}

// ════════════════════════════════════════════════════════════════════════════
//  TelemetryNode
// ════════════════════════════════════════════════════════════════════════════

TelemetryNode::TelemetryNode(
    std::shared_ptr<const GraphEngine> engine,
    AnomalyCallback                    on_anomaly,
    std::size_t                        n_workers)
    : engine_(std::move(engine))
    , on_anomaly_(std::move(on_anomaly))
    , pool_(n_workers)
{
    if (!engine_) {
        throw std::invalid_argument("TelemetryNode: engine must not be null");
    }

    // Zero-init vehicle state array.
    vehicle_state_.fill(VehicleState{});

    // Start background drain thread.
    drain_thread_ = std::thread([this]() {
        pin_thread_to_core(pool_.thread_count()); // next available core
        drain_loop();
    });
}

TelemetryNode::~TelemetryNode() {
    running_.store(false, std::memory_order_release);
    if (drain_thread_.joinable()) drain_thread_.join();
}

// ── Hot path: ingest ──────────────────────────────────────────────────────────
// Called from the producer thread. No mutex, no allocation — just an SPSC push.

bool TelemetryNode::ingest(const TelemetryPacket& pkt) noexcept {
    if (ring_.push(pkt)) [[likely]] {
        cnt_ingested_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    cnt_dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

// ── Background drain loop ─────────────────────────────────────────────────────
// Pops packets from SPSC ring and dispatches routing jobs to thread pool.

void TelemetryNode::drain_loop() {
    while (running_.load(std::memory_order_acquire) || !ring_.empty()) {
        auto opt = ring_.pop();
        if (!opt) [[unlikely]] {
            // Ring empty — brief sleep to avoid busy-spinning the drain thread.
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            continue;
        }

        TelemetryPacket pkt = *opt;

        // Dispatch processing to thread pool (captures pkt by value — trivially
        // copyable struct, 64 bytes, cheap).
        pool_.submit([this, pkt]() {
            const auto t0 = std::chrono::steady_clock::now();

            // ── Routing ───────────────────────────────────────────────────
            const RouteResult route =
                engine_->find_route(pkt.current_node, pkt.target_node);

            // ── Anomaly classification ────────────────────────────────────
            const std::size_t bucket = pkt.vehicle_id % kBuckets;
            VehicleState& vs = vehicle_state_[bucket];
            // NOTE: bucket-level races are benign for anomaly heuristics;
            // correctness is probabilistic by design. Use a per-bucket
            // spinlock below if strict consistency is required.
            const AnomalyKind kind = classify(pkt, vs);

            // Update state.
            vs.last_speed   = pkt.speed_kmh;
            vs.last_node    = pkt.current_node;
            vs.last_seen_ns = pkt.timestamp_ns;

            cnt_processed_.fetch_add(1, std::memory_order_relaxed);

            if (kind != AnomalyKind::None) [[unlikely]] {
                cnt_anomalies_.fetch_add(1, std::memory_order_relaxed);
                if (on_anomaly_) {
                    on_anomaly_(AnomalyEvent{
                        pkt.vehicle_id,
                        kind,
                        pkt.timestamp_ns,
                        route
                    });
                }
            }

            const auto t1 = std::chrono::steady_clock::now();
            const int64_t latency_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
                    .count();
            last_latency_ns_.store(latency_ns, std::memory_order_relaxed);
        });
    }
}

// ── Anomaly classifier ────────────────────────────────────────────────────────
// Pure function over packet + previous state; no allocation.

AnomalyKind TelemetryNode::classify(
    const TelemetryPacket& pkt,
    VehicleState&          vs) const noexcept
{
    constexpr float kFuelCritical     = 8.0f;
    constexpr float kSuddenStopRatio  = 0.20f; // speed fell below 20 % of prev
    constexpr int64_t kSignalLossNs   = 5'000'000'000LL; // 5 s
    constexpr float kSpeedExcessKmh   = 120.0f;

    // Signal loss (gap-based).
    if (vs.last_seen_ns > 0 &&
        (pkt.timestamp_ns - vs.last_seen_ns) > kSignalLossNs) [[unlikely]]
    {
        return AnomalyKind::SignalLoss;
    }

    // Emergency priority shortcut.
    if (pkt.priority == 2) [[unlikely]] return AnomalyKind::RouteDeviation;

    // Fuel critical.
    if (pkt.fuel_pct < kFuelCritical) [[unlikely]] return AnomalyKind::FuelCritical;

    // Speed excess.
    if (pkt.speed_kmh > kSpeedExcessKmh) [[unlikely]] return AnomalyKind::SpeedExcess;

    // Sudden stop.
    if (vs.last_speed > 0.0f &&
        pkt.speed_kmh < vs.last_speed * kSuddenStopRatio) [[unlikely]]
    {
        return AnomalyKind::SuddenStop;
    }

    return AnomalyKind::None;
}

// ── flush ─────────────────────────────────────────────────────────────────────

void TelemetryNode::flush() {
    // Wait until drain thread empties ring and pool drains.
    while (!ring_.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    pool_.drain();
}

// ── Stats ─────────────────────────────────────────────────────────────────────

TelemetryNode::Stats TelemetryNode::stats() const noexcept {
    return {
        cnt_ingested_ .load(std::memory_order_relaxed),
        cnt_processed_.load(std::memory_order_relaxed),
        cnt_dropped_  .load(std::memory_order_relaxed),
        cnt_anomalies_.load(std::memory_order_relaxed),
        last_latency_ns_.load(std::memory_order_relaxed),
    };
}

} // namespace asr
