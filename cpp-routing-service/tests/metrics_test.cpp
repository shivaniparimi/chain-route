// cpp-routing-service/tests/metrics_test.cpp
#include "metrics.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

// Extracts the integer value of "<metricLine>{le="<le>"} <value>" from a
// PrometheusText() dump. Fails the calling test (via ADD_FAILURE) if the
// series isn't present, rather than silently returning a bogus default.
uint64_t BucketValue(const std::string& text, const std::string& metric, const std::string& le) {
    std::string needle = metric + "{le=\"" + le + "\"} ";
    size_t pos = text.find(needle);
    if (pos == std::string::npos) {
        ADD_FAILURE() << "series not found: " << needle;
        return 0;
    }
    size_t valueStart = pos + needle.size();
    size_t valueEnd = text.find('\n', valueStart);
    return std::stoull(text.substr(valueStart, valueEnd - valueStart));
}

}  // namespace

TEST(RouteMetricsTest, StartsAtZero) {
    chainroute::RouteMetrics m;
    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_requests_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_no_route_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_errors_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordSuccessIncrementsRequestsAndSuccessfulRoutes) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 5, 0.002);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_requests_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_no_route_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordNoRouteIncrementsNoRouteCounter) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kNoRoute, 3, 0.001);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_no_route_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordErrorIncrementsErrorCounter) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kError, 0, 0.0005);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_errors_total 1"), std::string::npos);
}

TEST(RouteMetricsTest, PrometheusTextIsWellFormedExpositionFormat) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 4, 0.01);
    std::string text = m.PrometheusText();

    // Every metric line has a "# HELP" and "# TYPE" line before its
    // value line -- the minimum Prometheus text-format contract scrapers
    // rely on to know each series' type.
    EXPECT_NE(text.find("# HELP chainroute_router_requests_total"), std::string::npos);
    EXPECT_NE(text.find("# TYPE chainroute_router_requests_total counter"), std::string::npos);
    EXPECT_NE(text.find("# TYPE chainroute_router_duration_seconds histogram"), std::string::npos);
}

// Regression test for the double-cumulation bug: PrometheusText() summed an
// already-cumulative running total on top of itself, so a single
// observation would inflate every larger bucket instead of landing in only
// the buckets whose threshold it actually satisfies.
TEST(RouteMetricsTest, HistogramBucketsAreValidCumulativeProm_SingleObservation) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    // 0.5s is <= only the le="1" threshold among {0.001 .. 0.1, 1.0}, so
    // every smaller finite bucket must stay at 0 and le="1" must be exactly 1.
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 3, 0.5);

    std::string text = m.PrometheusText();
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.001"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.002"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.005"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.01"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.02"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.05"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "0.1"), 0u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "1"), 1u);
    EXPECT_EQ(BucketValue(text, "chainroute_router_duration_seconds_bucket", "+Inf"), 1u);
}

// With several observations spanning multiple buckets, this asserts the two
// invariants any valid cumulative Prometheus histogram must satisfy: no
// finite le= bucket may exceed the +Inf/count bucket, and bucket values must
// be monotonically non-decreasing as le increases. Under the old
// double-cumulation bug this would have failed both (le="1" would read 15,
// far above the true +Inf count of 3).
TEST(RouteMetricsTest, HistogramBucketsAreValidCumulativeProm_MultipleObservations) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 1, 0.0005);  // lands in every bucket
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 2, 0.003);  // lands in buckets >= 0.005
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 3, 0.5);    // lands only in le="1"

    std::string text = m.PrometheusText();
    const char* metric = "chainroute_router_duration_seconds_bucket";
    std::vector<uint64_t> values = {
        BucketValue(text, metric, "0.001"), BucketValue(text, metric, "0.002"),
        BucketValue(text, metric, "0.005"), BucketValue(text, metric, "0.01"),
        BucketValue(text, metric, "0.02"),  BucketValue(text, metric, "0.05"),
        BucketValue(text, metric, "0.1"),   BucketValue(text, metric, "1"),
    };
    uint64_t totalCount = BucketValue(text, metric, "+Inf");
    ASSERT_EQ(totalCount, 3u);

    // Exact expected counts, hand-derived from which thresholds each
    // observation satisfies.
    std::vector<uint64_t> expected = {1, 1, 2, 2, 2, 2, 2, 3};
    EXPECT_EQ(values, expected);

    // Fundamental Prometheus histogram invariants, checked directly rather
    // than just against the hand-derived numbers above.
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_LE(values[i], totalCount) << "finite bucket " << i << " exceeds +Inf/count total";
        if (i > 0) {
            EXPECT_GE(values[i], values[i - 1]) << "bucket " << i << " is not monotonically non-decreasing";
        }
    }
}

// Same double-cumulation bug, same fix, applied to the candidate-edge-count
// histogram -- verify it independently since it has its own bucket array
// and its own (previously separately-buggy) accumulator in PrometheusText().
TEST(RouteMetricsTest, EdgeHistogramBucketsAreValidCumulativeProm) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 0, 0.001);  // lands in every bucket (0,1,2,5,10)
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 3, 0.001);  // lands in buckets >= 5
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 8, 0.001);  // lands only in le="10"

    std::string text = m.PrometheusText();
    const char* metric = "chainroute_router_candidate_edges_bucket";
    std::vector<uint64_t> values = {
        BucketValue(text, metric, "0"), BucketValue(text, metric, "1"), BucketValue(text, metric, "2"),
        BucketValue(text, metric, "5"), BucketValue(text, metric, "10"),
    };
    uint64_t totalCount = BucketValue(text, metric, "+Inf");
    ASSERT_EQ(totalCount, 3u);

    std::vector<uint64_t> expected = {1, 1, 1, 2, 3};
    EXPECT_EQ(values, expected);

    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_LE(values[i], totalCount) << "finite bucket " << i << " exceeds +Inf/count total";
        if (i > 0) {
            EXPECT_GE(values[i], values[i - 1]) << "bucket " << i << " is not monotonically non-decreasing";
        }
    }
}
