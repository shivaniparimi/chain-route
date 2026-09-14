#pragma once

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute/v1/routing.grpc.pb.h"

namespace chainroute_service {

class RoutingServiceImpl final : public chainroute::v1::RoutingService::Service {
public:
    explicit RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator);

    grpc::Status FindRoute(grpc::ServerContext* context,
                            const chainroute::v1::FindRouteRequest* request,
                            chainroute::v1::FindRouteResponse* response) override;

private:
    chainroute::sim::NetworkSimulator& simulator_;
};

}  // namespace chainroute_service
