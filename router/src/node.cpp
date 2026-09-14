#include "chainroute/node.hpp"

#include <cstdint>
#include <functional>

namespace chainroute {

std::size_t NodeHash::operator()(const Node& node) const noexcept {
    const auto combined = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(node.chain) << 8) |
        static_cast<std::uint16_t>(node.asset));
    return std::hash<std::uint16_t>{}(combined);
}

std::string toString(const Node& node) {
    return toString(node.chain) + "-" + toString(node.asset);
}

}  // namespace chainroute
