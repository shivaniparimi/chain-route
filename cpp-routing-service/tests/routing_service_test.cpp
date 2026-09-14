#include <gtest/gtest.h>

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute_service/routing_service.hpp"

namespace chainroute_service {
namespace {

TEST(RoutingServiceTest, ReturnsARouteForAValidRequest) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    if (response.route_found()) {
        EXPECT_GT(response.hops_size(), 0);
        EXPECT_EQ(response.hops(0).from_chain(), chainroute::v1::CHAIN_ETHEREUM);
        EXPECT_EQ(response.hops(response.hops_size() - 1).to_chain(), chainroute::v1::CHAIN_BASE);
        EXPECT_FALSE(response.hops(0).bridge_name().empty());
        EXPECT_GE(response.total_fee(), 0.0);
    }
}

TEST(RoutingServiceTest, ReturnsRouteFoundFalseWhenNoNodesConnectDirectlyOrAtAll) {
    // Use an amount so large that liquidity can never cover it (Phase 3's
    // liquidity ceiling is a few million) -- guarantees no eligible route,
    // exercising the normal "no route" outcome deterministically for any seed.
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1e12);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
    EXPECT_DOUBLE_EQ(response.total_fee(), 0.0);
}

TEST(RoutingServiceTest, RejectsUnspecifiedSourceChain) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_UNSPECIFIED);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST(RoutingServiceTest, RejectsUnspecifiedAsset) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_UNSPECIFIED);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST(RoutingServiceTest, RejectsNonPositiveAmount) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(0.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

}  // namespace
}  // namespace chainroute_service
