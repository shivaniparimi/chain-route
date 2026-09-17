#pragma once

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute/v1/routing.grpc.pb.h"
#include "metrics.hpp"

namespace chainroute_service {

class RoutingServiceImpl final : public chainroute::v1::RoutingService::Service {
public:
    // Single-arg overload delegates to a process-wide default RouteMetrics
    // instance, preserving every existing call site (tests included) that
    // predates Phase 10 metrics instrumentation. Callers that care about
    // observing metrics (main.cpp) use the two-arg overload instead.
    explicit RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator);
    RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator, chainroute::RouteMetrics& metrics);

    grpc::Status FindRoute(grpc::ServerContext* context,
                            const chainroute::v1::FindRouteRequest* request,
                            chainroute::v1::FindRouteResponse* response) override;

private:
    chainroute::sim::NetworkSimulator& simulator_;
    chainroute::RouteMetrics& metrics_;
};

}  // namespace chainroute_service
