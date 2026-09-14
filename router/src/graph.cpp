#include "chainroute/graph.hpp"

#include <stdexcept>
#include <utility>

namespace chainroute {

NodeIndex Graph::addNode(const Node& node) {
    if (const auto existing = findNode(node)) {
        return *existing;
    }
    nodes_.push_back(node);
    adjacency_.emplace_back();
    const NodeIndex index = nodes_.size() - 1;
    nodeIndex_.emplace(node, index);
    return index;
}

std::optional<NodeIndex> Graph::findNode(const Node& node) const {
    const auto it = nodeIndex_.find(node);
    if (it == nodeIndex_.end()) {
        return std::nullopt;
    }
    return it->second;
}

const Node& Graph::nodeAt(NodeIndex idx) const {
    if (idx >= nodes_.size()) {
        throw std::out_of_range("Graph::nodeAt: index out of range");
    }
    return nodes_[idx];
}

std::size_t Graph::nodeCount() const {
    return nodes_.size();
}

void Graph::addEdge(NodeIndex from, Edge edge) {
    if (from >= nodes_.size()) {
        throw std::out_of_range("Graph::addEdge: 'from' index out of range");
    }
    if (edge.to >= nodes_.size()) {
        throw std::out_of_range("Graph::addEdge: 'to' index out of range");
    }
    adjacency_[from].push_back(std::move(edge));
}

const std::vector<Edge>& Graph::edgesFrom(NodeIndex idx) const {
    if (idx >= nodes_.size()) {
        throw std::out_of_range("Graph::edgesFrom: index out of range");
    }
    return adjacency_[idx];
}

std::size_t Graph::edgeCount() const {
    std::size_t count = 0;
    for (const auto& edges : adjacency_) {
        count += edges.size();
    }
    return count;
}

}  // namespace chainroute
