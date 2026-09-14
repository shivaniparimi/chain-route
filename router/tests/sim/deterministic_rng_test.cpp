#include <gtest/gtest.h>

#include "chainroute/sim/deterministic_rng.hpp"

namespace chainroute::sim {
namespace {

TEST(DeterministicRngTest, ResultAlwaysInUnitInterval) {
    for (std::uint64_t seed = 0; seed < 50; ++seed) {
        for (std::uint64_t a = 0; a < 5; ++a) {
            const double value = draw(seed, "range.check", a, seed, a * 3, seed + a);
            EXPECT_GE(value, 0.0);
            EXPECT_LT(value, 1.0);
        }
    }
}

TEST(DeterministicRngTest, SameInputsProduceSameOutput) {
    EXPECT_DOUBLE_EQ(draw(42, "repeat.check", 1, 2, 3, 4),
                      draw(42, "repeat.check", 1, 2, 3, 4));
}

TEST(DeterministicRngTest, DifferentArgumentPositionsProduceDifferentOutput) {
    EXPECT_NE(draw(42, "position.check", 1, 2, 0, 0),
              draw(42, "position.check", 2, 1, 0, 0));
}

TEST(DeterministicRngTest, DifferentRolesProduceDifferentOutput) {
    EXPECT_NE(draw(42, "role.a", 1, 2, 3, 4), draw(42, "role.b", 1, 2, 3, 4));
}

TEST(DeterministicRngTest, DifferentSeedsProduceDifferentOutput) {
    EXPECT_NE(draw(42, "seed.check", 1, 2, 3, 4), draw(43, "seed.check", 1, 2, 3, 4));
}

TEST(DeterministicRngTest, GoldenValues) {
    // Pinned from a real run of draw() -- see Task 1 Step 6 of the plan
    // for how these were generated. If deterministic_rng.cpp's algorithm
    // ever changes intentionally, regenerate and update these literals.
    EXPECT_DOUBLE_EQ(draw(1, "golden.a", 10, 20, 30, 40), 0.91901422550099487);
    EXPECT_DOUBLE_EQ(draw(2, "golden.a", 10, 20, 30, 40), 0.98482097151721171);
    EXPECT_DOUBLE_EQ(draw(1, "golden.b", 10, 20, 30, 40), 0.23115041900640731);
    EXPECT_DOUBLE_EQ(draw(1, "golden.a", 99, 20, 30, 40), 0.17363444482541768);
}

}  // namespace
}  // namespace chainroute::sim
