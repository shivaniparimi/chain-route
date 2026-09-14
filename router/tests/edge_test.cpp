#include <gtest/gtest.h>

#include "chainroute/edge.hpp"

namespace chainroute {
namespace {

TEST(EdgeTest, StoresAllFields) {
    const Edge edge{
        /*to=*/3,
        /*bridgeName=*/"Stargate",
        /*fee=*/1.5,
        /*latencyMs=*/2000.0,
        /*liquidity=*/1000000.0,
        /*reliability=*/0.99,
    };

    EXPECT_EQ(edge.to, 3u);
    EXPECT_EQ(edge.bridgeName, "Stargate");
    EXPECT_DOUBLE_EQ(edge.fee, 1.5);
    EXPECT_DOUBLE_EQ(edge.latencyMs, 2000.0);
    EXPECT_DOUBLE_EQ(edge.liquidity, 1000000.0);
    EXPECT_DOUBLE_EQ(edge.reliability, 0.99);
}

}  // namespace
}  // namespace chainroute
