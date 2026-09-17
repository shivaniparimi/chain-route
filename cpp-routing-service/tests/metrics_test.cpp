// cpp-routing-service/tests/metrics_test.cpp
#include "metrics.hpp"

#include <gtest/gtest.h>

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
