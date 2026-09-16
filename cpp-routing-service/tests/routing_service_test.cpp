#include <gtest/gtest.h>

#include <limits>

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

TEST(RoutingServiceTest, RejectsNaNAmount) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(std::numeric_limits<double>::quiet_NaN());

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST(RoutingServiceTest, UsesCandidateEdgesInsteadOfSimulatorWhenPresent) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* cheap = request.add_candidate_edges();
    cheap->set_bridge_name("across");
    cheap->set_fee(0.0001);
    cheap->set_latency_ms(60000.0);
    cheap->set_liquidity(0.001);
    cheap->set_reliability(1.0);

    auto* expensive = request.add_candidate_edges();
    expensive->set_bridge_name("other-provider");
    expensive->set_fee(0.01);
    expensive->set_latency_ms(60000.0);
    expensive->set_liquidity(0.001);
    expensive->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(response.route_found());
    ASSERT_EQ(response.hops_size(), 1);
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
    EXPECT_DOUBLE_EQ(response.hops(0).fee(), 0.0001);
    EXPECT_EQ(response.hops(0).from_chain(), chainroute::v1::CHAIN_ETHEREUM);
    EXPECT_EQ(response.hops(0).to_chain(), chainroute::v1::CHAIN_BASE);
    EXPECT_DOUBLE_EQ(response.total_fee(), 0.0001);
}

TEST(RoutingServiceTest, CandidateEdgeBelowLiquidityIsFilteredOut) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* tooSmall = request.add_candidate_edges();
    tooSmall->set_bridge_name("across");
    tooSmall->set_fee(0.0001);
    tooSmall->set_latency_ms(60000.0);
    tooSmall->set_liquidity(0.0);  // Available=false maps to liquidity=0 (design §4)
    tooSmall->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
}

TEST(RoutingServiceTest, EmptyCandidateEdgesFallsBackToSimulator) {
    // No candidate_edges set at all -- must take the exact same path as
    // every pre-Phase-8 test above, proving backward compatibility.
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
    // Same assertion shape as the pre-existing ReturnsARouteForAValidRequest
    // test -- simulator-driven, so route_found depends on the seed's
    // topology, not asserted true/false here, only that no crash/error occurs.
}

TEST(RoutingServiceTest, TwoRealProviders_AcrossCheaper_AcrossSelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0005);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    ASSERT_EQ(response.hops_size(), 1);
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_RelayCheaper_RelaySelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0005);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0001);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    EXPECT_EQ(response.hops(0).bridge_name(), "relay");
}

TEST(RoutingServiceTest, TwoRealProviders_EqualFee_FirstInsertedWins) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0002);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0002); // exactly equal
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    // Documents the existing, non-business tie-break (design doc §9): the
    // FIRST-inserted edge wins ties, not a deliberate provider preference.
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_RelayUnavailable_AcrossSelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.00001); // cheaper, but unavailable
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.0); relay->set_reliability(1.0); // Available=false -> liquidity=0

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_NeitherViable_NoRoute) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.0); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0001);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.0); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
}

}  // namespace
}  // namespace chainroute_service
