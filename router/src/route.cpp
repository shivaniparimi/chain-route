#include "chainroute/route.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <queue>
#include <stdexcept>
#include <utility>

namespace chainroute {

std::optional<Route> findCheapestRoute(
    const Graph& graph, NodeIndex source, NodeIndex destination, double amount) {
    const std::size_t n = graph.nodeCount();
    if (source >= n) {
        throw std::out_of_range("findCheapestRoute: source index out of range");
    }
    if (destination >= n) {
        throw std::out_of_range("findCheapestRoute: destination index out of range");
    }

    constexpr double kInfinity = std::numeric_limits<double>::infinity();
    std::vector<double> dist(n, kInfinity);
    std::vector<std::optional<NodeIndex>> predNode(n);
    std::vector<std::optional<Edge>> predEdge(n);
    dist[source] = 0.0;

    using QueueEntry = std::pair<double, NodeIndex>;
    std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<>> queue;
    queue.emplace(0.0, source);

    while (!queue.empty()) {
        const auto [d, u] = queue.top();
        queue.pop();

        if (d > dist[u]) {
            continue;
        }

        for (const Edge& edge : graph.edgesFrom(u)) {
            if (edge.liquidity < amount) {
                continue;
            }
            const double candidate = dist[u] + edge.fee;
            if (candidate < dist[edge.to]) {
                dist[edge.to] = candidate;
                predNode[edge.to] = u;
                predEdge[edge.to] = edge;
                queue.emplace(candidate, edge.to);
            }
        }
    }

    if (dist[destination] == kInfinity) {
        return std::nullopt;
    }

    std::vector<Edge> edges;
    NodeIndex current = destination;
    while (current != source) {
        edges.push_back(*predEdge[current]);
        current = *predNode[current];
    }
    std::reverse(edges.begin(), edges.end());

    return Route{std::move(edges), dist[destination]};
}

}  // namespace chainroute
