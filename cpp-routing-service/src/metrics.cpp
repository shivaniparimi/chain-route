#include "metrics.hpp"

#include <chrono>
#include <sstream>

namespace chainroute {

void RouteMetrics::RecordRequest() { requests_total_.fetch_add(1, std::memory_order_relaxed); }

void RouteMetrics::RecordOutcome(RouteOutcome outcome, int candidateEdgeCount, double durationSeconds) {
    switch (outcome) {
        case RouteOutcome::kSuccess:
            successful_routes_total_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RouteOutcome::kNoRoute:
            no_route_total_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RouteOutcome::kError:
            errors_total_.fetch_add(1, std::memory_order_relaxed);
            break;
    }

    latency_count_.fetch_add(1, std::memory_order_relaxed);
    double expectedSum = latency_sum_.load(std::memory_order_relaxed);
    while (!latency_sum_.compare_exchange_weak(expectedSum, expectedSum + durationSeconds, std::memory_order_relaxed)) {
    }
    for (int i = 0; i < kLatencyBucketCount; ++i) {
        if (durationSeconds <= kLatencyBucketsSeconds[i]) {
            latency_bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
        }
    }

    edge_count_.fetch_add(1, std::memory_order_relaxed);
    double expectedEdgeSum = edge_sum_.load(std::memory_order_relaxed);
    while (!edge_sum_.compare_exchange_weak(expectedEdgeSum, expectedEdgeSum + candidateEdgeCount, std::memory_order_relaxed)) {
    }
    for (int i = 0; i < kEdgeBucketCount; ++i) {
        if (candidateEdgeCount <= kEdgeBuckets[i]) {
            edge_bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
        }
    }
}

std::string RouteMetrics::PrometheusText() const {
    std::ostringstream out;
    out << "# HELP chainroute_router_requests_total Total FindRoute requests.\n";
    out << "# TYPE chainroute_router_requests_total counter\n";
    out << "chainroute_router_requests_total " << requests_total_.load() << "\n";

    out << "# HELP chainroute_router_successful_routes_total Total requests that found a route.\n";
    out << "# TYPE chainroute_router_successful_routes_total counter\n";
    out << "chainroute_router_successful_routes_total " << successful_routes_total_.load() << "\n";

    out << "# HELP chainroute_router_no_route_total Total requests with no viable route.\n";
    out << "# TYPE chainroute_router_no_route_total counter\n";
    out << "chainroute_router_no_route_total " << no_route_total_.load() << "\n";

    out << "# HELP chainroute_router_errors_total Total requests that errored.\n";
    out << "# TYPE chainroute_router_errors_total counter\n";
    out << "chainroute_router_errors_total " << errors_total_.load() << "\n";

    out << "# HELP chainroute_router_duration_seconds FindRoute latency.\n";
    out << "# TYPE chainroute_router_duration_seconds histogram\n";
    uint64_t cumulative = 0;
    for (int i = 0; i < kLatencyBucketCount; ++i) {
        cumulative += latency_bucket_counts_[i].load();
        out << "chainroute_router_duration_seconds_bucket{le=\"" << kLatencyBucketsSeconds[i] << "\"} " << cumulative << "\n";
    }
    out << "chainroute_router_duration_seconds_bucket{le=\"+Inf\"} " << latency_count_.load() << "\n";
    out << "chainroute_router_duration_seconds_sum " << latency_sum_.load() << "\n";
    out << "chainroute_router_duration_seconds_count " << latency_count_.load() << "\n";

    out << "# HELP chainroute_router_candidate_edges Candidate edge count per request.\n";
    out << "# TYPE chainroute_router_candidate_edges histogram\n";
    uint64_t edgeCumulative = 0;
    for (int i = 0; i < kEdgeBucketCount; ++i) {
        edgeCumulative += edge_bucket_counts_[i].load();
        out << "chainroute_router_candidate_edges_bucket{le=\"" << kEdgeBuckets[i] << "\"} " << edgeCumulative << "\n";
    }
    out << "chainroute_router_candidate_edges_bucket{le=\"+Inf\"} " << edge_count_.load() << "\n";
    out << "chainroute_router_candidate_edges_sum " << edge_sum_.load() << "\n";
    out << "chainroute_router_candidate_edges_count " << edge_count_.load() << "\n";

    return out.str();
}

MetricsScope::MetricsScope(RouteMetrics& metrics, int candidateEdgeCount)
    : metrics_(metrics), candidate_edge_count_(candidateEdgeCount), start_(std::chrono::steady_clock::now()) {
    metrics_.RecordRequest();
}

MetricsScope::~MetricsScope() {
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_).count();
    metrics_.RecordOutcome(outcome_, candidate_edge_count_, elapsed);
}

void MetricsScope::SetOutcome(RouteOutcome outcome) { outcome_ = outcome; }

}  // namespace chainroute
