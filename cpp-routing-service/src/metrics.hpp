#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace chainroute {

enum class RouteOutcome { kSuccess, kNoRoute, kError };

// RouteMetrics holds hand-rolled, lock-free counters and a fixed-bucket
// latency histogram for the routing service. No routing/Dijkstra logic
// lives here -- this is purely observational bookkeeping the service
// layer updates around its existing FindRoute call, per Phase 10 design
// doc §4 ("no large dependency for a trivial metric").
class RouteMetrics {
public:
    static constexpr int kLatencyBucketCount = 8;
    // Upper bounds in seconds: 1ms,2ms,5ms,10ms,20ms,50ms,100ms,+Inf(implicit last bucket)
    static constexpr std::array<double, kLatencyBucketCount> kLatencyBucketsSeconds = {
        0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 1.000};
    static constexpr int kEdgeBucketCount = 5;
    // Upper bounds for candidate-edge-count histogram: 0,1,2,5,10(+Inf implicit)
    static constexpr std::array<int, kEdgeBucketCount> kEdgeBuckets = {0, 1, 2, 5, 10};

    void RecordRequest();
    void RecordOutcome(RouteOutcome outcome, int candidateEdgeCount, double durationSeconds);

    // PrometheusText renders a snapshot of every metric in Prometheus
    // text exposition format (https://prometheus.io/docs/instrumenting/exposition_formats/).
    std::string PrometheusText() const;

private:
    std::atomic<uint64_t> requests_total_{0};
    std::atomic<uint64_t> successful_routes_total_{0};
    std::atomic<uint64_t> no_route_total_{0};
    std::atomic<uint64_t> errors_total_{0};

    std::array<std::atomic<uint64_t>, kLatencyBucketCount> latency_bucket_counts_{};
    std::atomic<uint64_t> latency_count_{0};
    std::atomic<double> latency_sum_{0.0};

    std::array<std::atomic<uint64_t>, kEdgeBucketCount> edge_bucket_counts_{};
    std::atomic<uint64_t> edge_count_{0};
    std::atomic<double> edge_sum_{0.0};
};

// MetricsScope is an RAII helper: construction records a request,
// destruction records the outcome/duration/candidate-edge-count based on
// whatever the caller set via SetOutcome before the scope ends. This lets
// FindRoute bracket its ENTIRE existing body (including every early-return
// validation branch) with a single declaration at the top, with zero
// changes to the Dijkstra call itself.
class MetricsScope {
public:
    MetricsScope(RouteMetrics& metrics, int candidateEdgeCount);
    ~MetricsScope();

    void SetOutcome(RouteOutcome outcome);

private:
    RouteMetrics& metrics_;
    int candidate_edge_count_;
    RouteOutcome outcome_ = RouteOutcome::kError; // default: an unset scope (e.g. an early throw) counts as an error, never as a silent success
    std::chrono::steady_clock::time_point start_;
};

}  // namespace chainroute
