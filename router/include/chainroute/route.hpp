#pragma once

#include <optional>
#include <vector>

#include "chainroute/edge.hpp"
#include "chainroute/graph.hpp"

namespace chainroute {

struct Route {
    std::vector<Edge> edges;
    double totalFee;
};

std::optional<Route> findCheapestRoute(
    const Graph& graph, NodeIndex source, NodeIndex destination, double amount);

}  // namespace chainroute
