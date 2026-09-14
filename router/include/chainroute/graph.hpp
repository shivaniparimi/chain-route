#pragma once

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

#include "chainroute/edge.hpp"
#include "chainroute/node.hpp"

namespace chainroute {

class Graph {
public:
    NodeIndex addNode(const Node& node);
    std::optional<NodeIndex> findNode(const Node& node) const;
    const Node& nodeAt(NodeIndex idx) const;

    void addEdge(NodeIndex from, Edge edge);
    const std::vector<Edge>& edgesFrom(NodeIndex idx) const;

    std::size_t nodeCount() const;
    std::size_t edgeCount() const;

private:
    std::vector<Node> nodes_;
    std::unordered_map<Node, NodeIndex, NodeHash> nodeIndex_;
    std::vector<std::vector<Edge>> adjacency_;
};

}  // namespace chainroute
