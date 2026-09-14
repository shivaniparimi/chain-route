#pragma once

#include <cstddef>
#include <string>

#include "chainroute/asset.hpp"
#include "chainroute/chain.hpp"

namespace chainroute {

struct Node {
    ChainId chain;
    AssetId asset;

    friend bool operator==(const Node&, const Node&) = default;
};

struct NodeHash {
    std::size_t operator()(const Node& node) const noexcept;
};

std::string toString(const Node& node);

}  // namespace chainroute
