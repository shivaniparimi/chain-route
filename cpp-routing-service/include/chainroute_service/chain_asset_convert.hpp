#pragma once

#include <optional>

#include "chainroute/asset.hpp"
#include "chainroute/chain.hpp"
#include "chainroute/v1/routing.pb.h"

namespace chainroute_service {

std::optional<chainroute::ChainId> toChainId(chainroute::v1::Chain proto);
std::optional<chainroute::AssetId> toAssetId(chainroute::v1::Asset proto);

chainroute::v1::Chain toProtoChain(chainroute::ChainId chain);
chainroute::v1::Asset toProtoAsset(chainroute::AssetId asset);

}  // namespace chainroute_service
