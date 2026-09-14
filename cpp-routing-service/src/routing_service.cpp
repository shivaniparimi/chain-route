#include "chainroute_service/routing_service.hpp"

#include "chainroute/route.hpp"
#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {

RoutingServiceImpl::RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator)
    : simulator_(simulator) {}

grpc::Status RoutingServiceImpl::FindRoute(
    grpc::ServerContext* /*context*/,
    const chainroute::v1::FindRouteRequest* request,
    chainroute::v1::FindRouteResponse* response) {
    const auto sourceChain = toChainId(request->source_chain());
    const auto destChain = toChainId(request->destination_chain());
    const auto asset = toAssetId(request->asset());

    if (!sourceChain.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid source_chain");
    }
    if (!destChain.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid destination_chain");
    }
    if (!asset.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid asset");
    }
    if (request->amount() <= 0.0) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "amount must be positive");
    }

    const chainroute::Graph graph = simulator_.snapshot();

    const auto sourceNode = graph.findNode(chainroute::Node{*sourceChain, *asset});
    const auto destNode = graph.findNode(chainroute::Node{*destChain, *asset});
    if (!sourceNode.has_value() || !destNode.has_value()) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "node not present in snapshot");
    }

    const auto route = chainroute::findCheapestRoute(graph, *sourceNode, *destNode, request->amount());

    if (!route.has_value()) {
        response->set_route_found(false);
        response->set_total_fee(0.0);
        return grpc::Status::OK;
    }

    response->set_route_found(true);
    response->set_total_fee(route->totalFee);

    chainroute::NodeIndex current = *sourceNode;
    for (const chainroute::Edge& edge : route->edges) {
        chainroute::v1::RouteHop* hop = response->add_hops();
        hop->set_from_chain(toProtoChain(graph.nodeAt(current).chain));
        hop->set_to_chain(toProtoChain(graph.nodeAt(edge.to).chain));
        hop->set_bridge_name(edge.bridgeName);
        hop->set_fee(edge.fee);
        hop->set_latency_ms(edge.latencyMs);
        hop->set_liquidity(edge.liquidity);
        hop->set_reliability(edge.reliability);
        current = edge.to;
    }

    return grpc::Status::OK;
}

}  // namespace chainroute_service
