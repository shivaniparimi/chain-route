# ChainRoute Phase 3 Network Simulator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `chainroute::sim::NetworkSimulator` per
`docs/superpowers/specs/2026-09-10-cpp-network-simulator-design.md`: a
seeded, deterministic generator of `chainroute::Graph` snapshots modeling 5
chains × {USDC, ETH}, with independently-determined directed bridge
availability and per-tick metric noise, backed by a fully-specified
portable RNG.

**Architecture:** Two new components under `router/*/sim/`: a pure
`draw(seed, role, a, b, c, d) -> double` mixing function
(`deterministic_rng`), and `NetworkSimulator` (`network_simulator`), which
generates a fixed `topology_` at construction and computes every metric on
demand inside `snapshot()` as a pure function of `(seed, bridge identity,
tick)`. No Phase 1/2 file is modified.

**Tech Stack:** C++20, existing CMake project, GoogleTest (already wired).

## Global Constraints

- Do NOT modify `router/include/chainroute/graph.hpp`, `node.hpp`,
  `edge.hpp`, `route.hpp`, `chain.hpp`, `asset.hpp`, or their `.cpp`
  files. Phase 1/2 behavior must be provably unchanged.
- Everything new lives in namespace `chainroute::sim`, under
  `router/include/chainroute/sim/` and `router/src/sim/`, registered into
  the existing single `chainroute` library and `chainroute_tests` binary.
- `NetworkSimulator` owns exactly three things: an immutable `seed_`, a
  mutable `tick_` (advanced only by `++tick_` in `tick()`), and a fixed
  `topology_` generated once at construction. No per-tick mutable metric
  state.
- `snapshot() const` builds and returns a brand-new `Graph` by value on
  every call — never a reference into simulator state, never mutates a
  previously-returned `Graph`.
- Directed bridge availability is independently determined per ordered
  chain pair — never guarantee that `A→B` existing implies `B→A` exists.
- Every metric draw is a pure function of `(seed, stable bridge identity,
  metric role, tick)` — never of loop/container iteration order.
- Never use `std::hash<std::string>`, `std::uniform_real_distribution` (or
  any `<random>` distribution), or unordered-container (`unordered_map`/
  `unordered_set`) iteration order as a source of identity or randomness
  anywhere in `chainroute::sim`. Topology generation iterates fixed
  `constexpr std::array`s only.
- The custom RNG (`deterministic_rng`) is minimal, fully specified in
  code, and documented as being for reproducible simulation, not
  cryptography — no claim of security properties.
- `NodeIndex` assignment must be identical across every snapshot, every
  seed, every tick (fixed 10-node universe, fixed generation order).
- Edge insertion order per adjacency bucket must be deterministic and
  documented (fixed topology-generation loop order: asset outer in
  `{USDC, ETH}`, then source chain, then target chain, both in
  `ChainId`'s 5-chain declaration order, then bridge index).
- Do NOT introduce a `GraphSource`-style abstract interface.
- Do NOT add multi-criteria routing (no `latencyMs`/`reliability` in any
  cost calculation).
- Do NOT add Go, Kafka, PostgreSQL, Redis, Docker, networking, or
  blockchain API code.
- Tests requiring a specific seed/tick/amount scenario (route flips,
  liquidity invalidation) must have that fixture found during
  development and pinned as literals — the committed test suite must
  contain no runtime seed-search loop.
- Metric valid ranges: `fee >= 0`, `latencyMs > 0`, `liquidity >= 0`,
  `0 <= reliability <= 1` — always clamped.

---

### Task 1: Deterministic RNG (`draw`) with golden-value tests

**Files:**
- Create: `router/include/chainroute/sim/deterministic_rng.hpp`
- Create: `router/src/sim/deterministic_rng.cpp`
- Create: `router/tests/sim/deterministic_rng_test.cpp`
- Modify: `router/CMakeLists.txt` (add `src/sim/deterministic_rng.cpp`)
- Modify: `router/tests/CMakeLists.txt` (add `sim/deterministic_rng_test.cpp`)

**Interfaces:**
- Produces:
```cpp
namespace chainroute::sim {
using Seed = std::uint64_t;
double draw(Seed seed, std::string_view role,
            std::uint64_t a = 0, std::uint64_t b = 0,
            std::uint64_t c = 0, std::uint64_t d = 0);
}
```

- [ ] **Step 1: Create the header**

`router/include/chainroute/sim/deterministic_rng.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <string_view>

namespace chainroute::sim {

using Seed = std::uint64_t;

// Deterministic, portable pseudo-random value in [0, 1) for a given
// (seed, role, a, b, c, d) combination. Same inputs always produce the
// same output on any conforming C++ compiler/standard library, because
// the implementation uses only basic integer arithmetic -- never
// std::hash<std::string> or std::uniform_real_distribution, whose
// algorithms are unspecified by the C++ standard and can differ across
// standard library implementations.
//
// This is for reproducible simulation, not cryptography: it has no
// security properties and must never be used to generate secrets.
double draw(Seed seed, std::string_view role,
            std::uint64_t a = 0, std::uint64_t b = 0,
            std::uint64_t c = 0, std::uint64_t d = 0);

}  // namespace chainroute::sim
```

- [ ] **Step 2: Create the implementation**

`router/src/sim/deterministic_rng.cpp`:

```cpp
#include "chainroute/sim/deterministic_rng.hpp"

namespace chainroute::sim {

namespace {

constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t fnv1aByte(std::uint64_t state, std::uint8_t byte) {
    state ^= byte;
    state *= kFnvPrime;
    return state;
}

// splitmix64 finalizer: cheap, well-distributed avalanche mixing.
std::uint64_t splitmix64(std::uint64_t state) {
    state += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

}  // namespace

double draw(Seed seed, std::string_view role,
            std::uint64_t a, std::uint64_t b, std::uint64_t c, std::uint64_t d) {
    std::uint64_t state = kFnvOffsetBasis;
    for (unsigned char byte : role) {
        state = fnv1aByte(state, byte);
    }
    for (int i = 0; i < 8; ++i) {
        state = fnv1aByte(state, static_cast<std::uint8_t>(seed >> (8 * i)));
    }

    state = splitmix64(state ^ a);
    state = splitmix64(state ^ b);
    state = splitmix64(state ^ c);
    state = splitmix64(state ^ d);

    // Top 53 bits give a double in [0, 1) with full mantissa precision.
    constexpr int kMantissaBits = 53;
    constexpr double kScale = 1.0 / static_cast<double>(1ULL << kMantissaBits);
    return static_cast<double>(state >> (64 - kMantissaBits)) * kScale;
}

}  // namespace chainroute::sim
```

- [ ] **Step 3: Register the new files in CMake**

In `router/CMakeLists.txt`, add `src/sim/deterministic_rng.cpp` to the
`add_library(chainroute STATIC ...)` source list.

In `router/tests/CMakeLists.txt`, add `sim/deterministic_rng_test.cpp` to
the `add_executable(chainroute_tests ...)` source list.

- [ ] **Step 4: Write the range/property test (no golden values needed)**

`router/tests/sim/deterministic_rng_test.cpp`:

```cpp
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

}  // namespace
}  // namespace chainroute::sim
```

- [ ] **Step 5: Build and run, verify these tests pass**

Run:

```bash
cmake -S router -B router/build
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=DeterministicRngTest.*
```

Expected: `PASSED` (5 tests).

- [ ] **Step 6: Generate and pin golden values**

These tests confirm exact, specific outputs never silently drift if the
mixing algorithm is later changed. The literal expected values cannot be
computed by hand — generate them by running the real implementation once.

Create a temporary scratch file (do NOT add it to CMake or commit it):

`/tmp/print_rng_golden.cpp`:

```cpp
#include <cstdio>

#include "chainroute/sim/deterministic_rng.hpp"

int main() {
    using chainroute::sim::draw;
    std::printf("%.17g\n", draw(1, "golden.a", 10, 20, 30, 40));
    std::printf("%.17g\n", draw(2, "golden.a", 10, 20, 30, 40));
    std::printf("%.17g\n", draw(1, "golden.b", 10, 20, 30, 40));
    std::printf("%.17g\n", draw(1, "golden.a", 99, 20, 30, 40));
    return 0;
}
```

Compile and run it against the real implementation from within `router/`:

```bash
g++ -std=c++20 -I include /tmp/print_rng_golden.cpp src/sim/deterministic_rng.cpp -o /tmp/print_rng_golden
/tmp/print_rng_golden
```

This prints 4 lines, each a `double` in `[0, 1)`. Copy them, in order,
into the four `EXPECT_DOUBLE_EQ` literals below. Add this test to
`router/tests/sim/deterministic_rng_test.cpp`, inside the existing
anonymous namespace:

```cpp
TEST(DeterministicRngTest, GoldenValues) {
    // Pinned from a real run of draw() -- see Task 1 Step 6 of the plan
    // for how these were generated. If deterministic_rng.cpp's algorithm
    // ever changes intentionally, regenerate and update these literals.
    EXPECT_DOUBLE_EQ(draw(1, "golden.a", 10, 20, 30, 40), /* value from line 1 */);
    EXPECT_DOUBLE_EQ(draw(2, "golden.a", 10, 20, 30, 40), /* value from line 2 */);
    EXPECT_DOUBLE_EQ(draw(1, "golden.b", 10, 20, 30, 40), /* value from line 3 */);
    EXPECT_DOUBLE_EQ(draw(1, "golden.a", 99, 20, 30, 40), /* value from line 4 */);
}
```

Replace each `/* value from line N */` with the actual printed literal
(e.g. `0.123456789...`). Delete `/tmp/print_rng_golden.cpp` and
`/tmp/print_rng_golden` afterward — they must not be committed.

- [ ] **Step 7: Build and run, verify the golden test passes**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=DeterministicRngTest.*
```

Expected: `PASSED` (6 tests).

- [ ] **Step 8: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/sim/deterministic_rng.hpp router/src/sim/deterministic_rng.cpp router/tests/sim/deterministic_rng_test.cpp
git commit -m "feat(router): add deterministic RNG for network simulation"
```

---

### Task 2: NetworkSimulator core (topology generation, snapshot, tick)

**Files:**
- Create: `router/include/chainroute/sim/network_simulator.hpp`
- Create: `router/src/sim/network_simulator.cpp`
- Create: `router/tests/sim/network_simulator_test.cpp`
- Modify: `router/CMakeLists.txt` (add `src/sim/network_simulator.cpp`)
- Modify: `router/tests/CMakeLists.txt` (add `sim/network_simulator_test.cpp`)

**Interfaces:**
- Consumes: `draw` (Task 1); `Graph`, `Node`, `Edge`, `NodeIndex`, `ChainId`,
  `AssetId` (Phase 1, unchanged).
- Produces:
```cpp
namespace chainroute::sim {
class NetworkSimulator {
public:
    explicit NetworkSimulator(Seed seed);
    void tick();
    std::uint64_t currentTick() const;
    Graph snapshot() const;
};
}
```

- [ ] **Step 1: Create the header**

`router/include/chainroute/sim/network_simulator.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "chainroute/graph.hpp"
#include "chainroute/sim/deterministic_rng.hpp"

namespace chainroute::sim {

// Internal: one bridge's fixed identity and base parameters, generated
// once at NetworkSimulator construction and never mutated. Not part of
// NetworkSimulator's public contract.
struct BridgeTemplate {
    Node source;
    Node target;
    std::string bridgeName;
    double baseFee;
    double baseLatencyMs;
    double baseLiquidity;
    double baseReliability;
    double volatility;
    std::uint64_t bridgeIndex;
};

class NetworkSimulator {
public:
    explicit NetworkSimulator(Seed seed);

    void tick();
    std::uint64_t currentTick() const;

    Graph snapshot() const;

private:
    Seed seed_;
    std::uint64_t tick_ = 0;
    std::vector<BridgeTemplate> topology_;
};

}  // namespace chainroute::sim
```

- [ ] **Step 2: Create the implementation**

`router/src/sim/network_simulator.cpp`:

```cpp
#include "chainroute/sim/network_simulator.hpp"

#include <array>
#include <limits>

namespace chainroute::sim {

namespace {

// Fixed generation order: asset outer in {USDC, ETH}, then chain in
// ChainId's 5-chain declaration order. Node creation in snapshot() and
// the topology-generation loops below both use this exact order, so
// NodeIndex assignment and edge insertion order are fully deterministic.
constexpr std::array<ChainId, 5> kChains = {
    ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
};
constexpr std::array<AssetId, 2> kAssets = {
    AssetId::USDC, AssetId::ETH,
};
constexpr std::array<const char*, 6> kBridgeNamePool = {
    "Stargate", "Wormhole", "Across", "Hop", "Synapse", "Celer",
};

constexpr double kPairInclusionProbability = 0.6;
constexpr int kMinBridgesPerPair = 1;
constexpr int kMaxBridgesPerPair = 3;

constexpr double kMinBaseFee = 0.1;
constexpr double kMaxBaseFee = 10.0;
constexpr double kMinBaseLatencyMs = 200.0;
constexpr double kMaxBaseLatencyMs = 5000.0;
constexpr double kMinBaseLiquidity = 10000.0;
constexpr double kMaxBaseLiquidity = 2000000.0;
constexpr double kMinBaseReliability = 0.85;
constexpr double kMaxBaseReliability = 0.999;
constexpr double kMinVolatility = 0.05;
constexpr double kMaxVolatility = 0.3;

// AssetId's underlying values are not contiguous over just {USDC, ETH}
// (USDC=0, ETH=2 in the full 5-value enum), so array indexing into the
// compact 2-element kAssets/nodeIndex arrays needs an explicit map --
// unlike ChainId, whose 5 values ARE contiguous 0..4 and can be cast
// directly.
std::size_t assetIndexOf(AssetId asset) {
    switch (asset) {
        case AssetId::USDC: return 0;
        case AssetId::ETH: return 1;
        default: return 0;
    }
}

// Combines an ordered chain pair into one stable identifier. Target < 8
// always (ChainId has 5 values), so this is injective.
std::uint64_t pairKey(ChainId source, ChainId target) {
    return static_cast<std::uint64_t>(source) * 8 + static_cast<std::uint64_t>(target);
}

double clamp(double value, double lo, double hi) {
    if (value < lo) return lo;
    if (value > hi) return hi;
    return value;
}

double scaledDraw(Seed seed, std::string_view role, std::uint64_t a, std::uint64_t b,
                   std::uint64_t c, std::uint64_t d, double lo, double hi) {
    return lo + draw(seed, role, a, b, c, d) * (hi - lo);
}

std::vector<BridgeTemplate> generateTopology(Seed seed) {
    std::vector<BridgeTemplate> topology;

    for (const AssetId asset : kAssets) {
        const auto assetArg = static_cast<std::uint64_t>(asset);

        for (const ChainId sourceChain : kChains) {
            for (const ChainId targetChain : kChains) {
                if (sourceChain == targetChain) {
                    continue;
                }
                const std::uint64_t pk = pairKey(sourceChain, targetChain);

                const double includeRoll = draw(seed, "topo.include", pk, assetArg, 0, 0);
                if (includeRoll >= kPairInclusionProbability) {
                    continue;
                }

                const double countRoll = draw(seed, "topo.count", pk, assetArg, 0, 0);
                const int span = kMaxBridgesPerPair - kMinBridgesPerPair + 1;
                const int bridgeCount = kMinBridgesPerPair + static_cast<int>(countRoll * span);

                for (int bridgeIndex = 0; bridgeIndex < bridgeCount; ++bridgeIndex) {
                    const auto idx = static_cast<std::uint64_t>(bridgeIndex);

                    const double nameRoll = draw(seed, "bridge.name", pk, assetArg, idx, 0);
                    const std::size_t nameIdx =
                        static_cast<std::size_t>(nameRoll * kBridgeNamePool.size());
                    std::string bridgeName = std::string(kBridgeNamePool[nameIdx]) + "#" +
                        std::to_string(bridgeIndex + 1);

                    const double baseFee = scaledDraw(
                        seed, "bridge.baseFee", pk, assetArg, idx, 0, kMinBaseFee, kMaxBaseFee);
                    const double baseLatencyMs = scaledDraw(
                        seed, "bridge.baseLatency", pk, assetArg, idx, 0,
                        kMinBaseLatencyMs, kMaxBaseLatencyMs);
                    const double baseLiquidity = scaledDraw(
                        seed, "bridge.baseLiquidity", pk, assetArg, idx, 0,
                        kMinBaseLiquidity, kMaxBaseLiquidity);
                    const double baseReliability = scaledDraw(
                        seed, "bridge.baseReliability", pk, assetArg, idx, 0,
                        kMinBaseReliability, kMaxBaseReliability);
                    const double volatility = scaledDraw(
                        seed, "bridge.volatility", pk, assetArg, idx, 0,
                        kMinVolatility, kMaxVolatility);

                    topology.push_back(BridgeTemplate{
                        Node{sourceChain, asset},
                        Node{targetChain, asset},
                        std::move(bridgeName),
                        baseFee,
                        baseLatencyMs,
                        baseLiquidity,
                        baseReliability,
                        volatility,
                        idx,
                    });
                }
            }
        }
    }

    return topology;
}

double tickMetric(Seed seed, const BridgeTemplate& bridge, std::string_view role,
                   double base, std::uint64_t tick, double lo, double hi) {
    const std::uint64_t pk = pairKey(bridge.source.chain, bridge.target.chain);
    const auto assetArg = static_cast<std::uint64_t>(bridge.source.asset);
    const double noiseRoll = draw(seed, role, pk, assetArg, bridge.bridgeIndex, tick);
    const double noise = noiseRoll * 2.0 - 1.0;  // maps [0,1) to [-1,1)
    return clamp(base + noise * base * bridge.volatility, lo, hi);
}

}  // namespace

NetworkSimulator::NetworkSimulator(Seed seed) : seed_(seed), topology_(generateTopology(seed)) {}

void NetworkSimulator::tick() {
    ++tick_;
}

std::uint64_t NetworkSimulator::currentTick() const {
    return tick_;
}

Graph NetworkSimulator::snapshot() const {
    Graph g;

    std::array<std::array<NodeIndex, kChains.size()>, kAssets.size()> nodeIndex{};
    for (std::size_t assetIdx = 0; assetIdx < kAssets.size(); ++assetIdx) {
        for (std::size_t chainIdx = 0; chainIdx < kChains.size(); ++chainIdx) {
            nodeIndex[assetIdx][chainIdx] = g.addNode(Node{kChains[chainIdx], kAssets[assetIdx]});
        }
    }

    for (const BridgeTemplate& bridge : topology_) {
        const std::size_t assetIdx = assetIndexOf(bridge.source.asset);
        const std::size_t sourceChainIdx = static_cast<std::size_t>(bridge.source.chain);
        const std::size_t targetChainIdx = static_cast<std::size_t>(bridge.target.chain);
        const NodeIndex sourceIndex = nodeIndex[assetIdx][sourceChainIdx];
        const NodeIndex targetIndex = nodeIndex[assetIdx][targetChainIdx];

        constexpr double kMax = std::numeric_limits<double>::max();
        const double fee = tickMetric(seed_, bridge, "tick.fee", bridge.baseFee, tick_, 0.0, kMax);
        const double latencyMs = tickMetric(
            seed_, bridge, "tick.latency", bridge.baseLatencyMs, tick_, 1.0, kMax);
        const double liquidity = tickMetric(
            seed_, bridge, "tick.liquidity", bridge.baseLiquidity, tick_, 0.0, kMax);
        const double reliability = tickMetric(
            seed_, bridge, "tick.reliability", bridge.baseReliability, tick_, 0.0, 1.0);

        g.addEdge(sourceIndex,
                   Edge{targetIndex, bridge.bridgeName, fee, latencyMs, liquidity, reliability});
    }

    return g;
}

}  // namespace chainroute::sim
```

- [ ] **Step 3: Register the new files in CMake**

In `router/CMakeLists.txt`, add `src/sim/network_simulator.cpp` to the
`add_library(chainroute STATIC ...)` source list.

In `router/tests/CMakeLists.txt`, add `sim/network_simulator_test.cpp` to
the `add_executable(chainroute_tests ...)` source list.

- [ ] **Step 4: Write the failing tests**

`router/tests/sim/network_simulator_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include <cstdint>

#include "chainroute/sim/network_simulator.hpp"

namespace chainroute::sim {
namespace {

TEST(NetworkSimulatorTest, SameSeedSameTickProducesIdenticalSnapshots) {
    NetworkSimulator a(1001);
    NetworkSimulator b(1001);

    const Graph ga = a.snapshot();
    const Graph gb = b.snapshot();

    ASSERT_EQ(ga.nodeCount(), gb.nodeCount());
    ASSERT_EQ(ga.edgeCount(), gb.edgeCount());
    for (NodeIndex i = 0; i < ga.nodeCount(); ++i) {
        const auto& edgesA = ga.edgesFrom(i);
        const auto& edgesB = gb.edgesFrom(i);
        ASSERT_EQ(edgesA.size(), edgesB.size());
        for (std::size_t j = 0; j < edgesA.size(); ++j) {
            EXPECT_EQ(edgesA[j].to, edgesB[j].to);
            EXPECT_EQ(edgesA[j].bridgeName, edgesB[j].bridgeName);
            EXPECT_DOUBLE_EQ(edgesA[j].fee, edgesB[j].fee);
            EXPECT_DOUBLE_EQ(edgesA[j].latencyMs, edgesB[j].latencyMs);
            EXPECT_DOUBLE_EQ(edgesA[j].liquidity, edgesB[j].liquidity);
            EXPECT_DOUBLE_EQ(edgesA[j].reliability, edgesB[j].reliability);
        }
    }
}

TEST(NetworkSimulatorTest, DifferentSeedsProduceDifferentSnapshots) {
    NetworkSimulator a(1001);
    NetworkSimulator b(2002);

    const Graph ga = a.snapshot();
    const Graph gb = b.snapshot();

    bool anyDifference = ga.edgeCount() != gb.edgeCount();
    if (!anyDifference) {
        for (NodeIndex i = 0; i < ga.nodeCount() && !anyDifference; ++i) {
            const auto& edgesA = ga.edgesFrom(i);
            const auto& edgesB = gb.edgesFrom(i);
            if (edgesA.size() != edgesB.size()) {
                anyDifference = true;
                break;
            }
            for (std::size_t j = 0; j < edgesA.size(); ++j) {
                if (edgesA[j].bridgeName != edgesB[j].bridgeName ||
                    edgesA[j].fee != edgesB[j].fee) {
                    anyDifference = true;
                    break;
                }
            }
        }
    }
    EXPECT_TRUE(anyDifference);
}

TEST(NetworkSimulatorTest, TickChangesMetricsButNotTopology) {
    NetworkSimulator sim(1001);
    const Graph g0 = sim.snapshot();
    sim.tick();
    const Graph g1 = sim.snapshot();

    ASSERT_EQ(g0.nodeCount(), g1.nodeCount());
    ASSERT_EQ(g0.edgeCount(), g1.edgeCount());

    bool anyMetricDifference = false;
    for (NodeIndex i = 0; i < g0.nodeCount(); ++i) {
        const auto& edges0 = g0.edgesFrom(i);
        const auto& edges1 = g1.edgesFrom(i);
        ASSERT_EQ(edges0.size(), edges1.size());
        for (std::size_t j = 0; j < edges0.size(); ++j) {
            EXPECT_EQ(edges0[j].to, edges1[j].to);
            EXPECT_EQ(edges0[j].bridgeName, edges1[j].bridgeName);
            if (edges0[j].fee != edges1[j].fee || edges0[j].latencyMs != edges1[j].latencyMs ||
                edges0[j].liquidity != edges1[j].liquidity ||
                edges0[j].reliability != edges1[j].reliability) {
                anyMetricDifference = true;
            }
        }
    }
    EXPECT_TRUE(anyMetricDifference);
}

TEST(NetworkSimulatorTest, NodeIndexStableAcrossSeedsAndTicks) {
    NetworkSimulator a(1001);
    NetworkSimulator b(2002);
    const Graph ga = a.snapshot();
    a.tick();
    const Graph ga2 = a.snapshot();
    const Graph gb = b.snapshot();

    const Node ethUsdc{ChainId::Ethereum, AssetId::USDC};
    const auto idxA = ga.findNode(ethUsdc);
    const auto idxA2 = ga2.findNode(ethUsdc);
    const auto idxB = gb.findNode(ethUsdc);
    ASSERT_TRUE(idxA.has_value());
    ASSERT_TRUE(idxA2.has_value());
    ASSERT_TRUE(idxB.has_value());
    EXPECT_EQ(*idxA, *idxA2);
    EXPECT_EQ(*idxA, *idxB);
}

TEST(NetworkSimulatorTest, OldSnapshotUnaffectedByLaterTicks) {
    NetworkSimulator sim(1001);
    const Graph g0 = sim.snapshot();

    double originalFee = -1.0;
    bool found = false;
    for (NodeIndex i = 0; i < g0.nodeCount() && !found; ++i) {
        const auto& edges = g0.edgesFrom(i);
        if (!edges.empty()) {
            originalFee = edges[0].fee;
            found = true;
        }
    }
    ASSERT_TRUE(found);

    for (int i = 0; i < 10; ++i) {
        sim.tick();
    }
    sim.snapshot();

    double stillOriginalFee = -1.0;
    for (NodeIndex i = 0; i < g0.nodeCount(); ++i) {
        const auto& edges = g0.edgesFrom(i);
        if (!edges.empty()) {
            stillOriginalFee = edges[0].fee;
            break;
        }
    }
    EXPECT_DOUBLE_EQ(originalFee, stillOriginalFee);
}

TEST(NetworkSimulatorTest, MetricsStayWithinValidBoundsAcrossManyTicks) {
    NetworkSimulator sim(1001);
    for (int t = 0; t < 1000; ++t) {
        const Graph g = sim.snapshot();
        for (NodeIndex i = 0; i < g.nodeCount(); ++i) {
            for (const Edge& edge : g.edgesFrom(i)) {
                EXPECT_GE(edge.fee, 0.0);
                EXPECT_GT(edge.latencyMs, 0.0);
                EXPECT_GE(edge.liquidity, 0.0);
                EXPECT_GE(edge.reliability, 0.0);
                EXPECT_LE(edge.reliability, 1.0);
            }
        }
        sim.tick();
    }
}

}  // namespace
}  // namespace chainroute::sim
```

- [ ] **Step 5: Build and run, verify all tests pass**

Run:

```bash
cmake -S router -B router/build
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=NetworkSimulatorTest.*
```

Expected: `PASSED` (6 tests). If `DifferentSeedsProduceDifferentSnapshots`
or `TickChangesMetricsButNotTopology` fails (astronomically unlikely, but
possible for a specific seed/args combination), change the seed literals
`1001`/`2002` consistently across this file to another fixed pair (e.g.
`3003`/`4004`) and re-run.

- [ ] **Step 6: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/sim/network_simulator.hpp router/src/sim/network_simulator.cpp router/tests/sim/network_simulator_test.cpp
git commit -m "feat(router): add NetworkSimulator topology generation and snapshot"
```

---

### Task 3: Structural multigraph and reachability tests

**Files:**
- Modify: `router/tests/sim/network_simulator_test.cpp` (add 3 tests; no
  production code changes expected)

**Interfaces:**
- Consumes: `NetworkSimulator` (Task 2); `findCheapestRoute` (Phase 2,
  unchanged) — `#include "chainroute/route.hpp"`.

- [ ] **Step 1: Write the tests**

Add to `router/tests/sim/network_simulator_test.cpp`, inside the existing
anonymous namespace. Add `#include <array>`, `#include <optional>`,
`#include <unordered_map>`, and `#include "chainroute/route.hpp"` to the
top of the file first.

```cpp
TEST(NetworkSimulatorTest, SomeChainPairHasAsymmetricBridgeAvailability) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    static constexpr std::array<ChainId, 5> kChains = {
        ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
    };

    bool foundAsymmetry = false;
    for (ChainId a : kChains) {
        for (ChainId b : kChains) {
            if (a == b) continue;
            const auto nodeA = g.findNode(Node{a, AssetId::USDC});
            const auto nodeB = g.findNode(Node{b, AssetId::USDC});
            ASSERT_TRUE(nodeA.has_value());
            ASSERT_TRUE(nodeB.has_value());

            bool aToB = false;
            for (const Edge& e : g.edgesFrom(*nodeA)) {
                if (e.to == *nodeB) { aToB = true; break; }
            }
            bool bToA = false;
            for (const Edge& e : g.edgesFrom(*nodeB)) {
                if (e.to == *nodeA) { bToA = true; break; }
            }
            if (aToB != bToA) {
                foundAsymmetry = true;
            }
        }
    }
    EXPECT_TRUE(foundAsymmetry);
}

TEST(NetworkSimulatorTest, SomeChainPairHasParallelBridges) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    bool foundParallel = false;
    for (NodeIndex i = 0; i < g.nodeCount() && !foundParallel; ++i) {
        std::unordered_map<NodeIndex, int> countByTarget;
        for (const Edge& e : g.edgesFrom(i)) {
            if (++countByTarget[e.to] > 1) {
                foundParallel = true;
                break;
            }
        }
    }
    EXPECT_TRUE(foundParallel);
}

TEST(NetworkSimulatorTest, SomeChainPairHasNoDirectBridgeAndRoutesMultiHopOrNullopt) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    static constexpr std::array<ChainId, 5> kChains = {
        ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
    };

    std::optional<NodeIndex> missingSource;
    std::optional<NodeIndex> missingTarget;
    for (ChainId a : kChains) {
        for (ChainId b : kChains) {
            if (a == b) continue;
            const auto nodeA = g.findNode(Node{a, AssetId::USDC});
            const auto nodeB = g.findNode(Node{b, AssetId::USDC});
            ASSERT_TRUE(nodeA.has_value());
            ASSERT_TRUE(nodeB.has_value());

            bool direct = false;
            for (const Edge& e : g.edgesFrom(*nodeA)) {
                if (e.to == *nodeB) { direct = true; break; }
            }
            if (!direct) {
                missingSource = nodeA;
                missingTarget = nodeB;
                break;
            }
        }
        if (missingSource) break;
    }
    ASSERT_TRUE(missingSource.has_value());
    ASSERT_TRUE(missingTarget.has_value());

    const auto route = findCheapestRoute(g, *missingSource, *missingTarget, 1000.0);
    if (route.has_value()) {
        EXPECT_GT(route->edges.size(), 1u);
    }
    // A nullopt result is also an acceptable, spec-sanctioned outcome here.
}
```

- [ ] **Step 2: Build and run, verify all three tests pass**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=NetworkSimulatorTest.*
```

Expected: `PASSED` (9 tests total). If seed `1001` doesn't happen to
produce the needed structural property for one of these tests (unlikely,
but possible), change that test's seed literal to another fixed value
(e.g. `1002`, `1003`, ...) and re-run — this is a one-time fixture choice,
not a search left in the suite.

- [ ] **Step 3: Commit**

```bash
git add router/tests/sim/network_simulator_test.cpp
git commit -m "test(router): cover directed asymmetry, parallel bridges, and sparse topology"
```

---

### Task 4: Route-flip-over-time and liquidity-invalidation scenario tests

**Files:**
- Modify: `router/tests/sim/network_simulator_test.cpp` (add 2 tests; no
  production code changes expected)

**Interfaces:**
- Consumes: `NetworkSimulator` (Task 2), `findCheapestRoute`/`Route`
  (Phase 2, unchanged).

These two scenarios need a *specific* seed and tick pair that happens to
produce the transition — unlike Task 3's properties, they are not close
to guaranteed for an arbitrary fixed seed. Find them with a temporary,
uncommitted search tool, then pin literals into the permanent test file.

- [ ] **Step 1: Write the temporary search tool**

Create `/tmp/find_fixtures.cpp` (do NOT add to CMake or commit):

```cpp
#include <cstdio>
#include <optional>
#include <vector>

#include "chainroute/route.hpp"
#include "chainroute/sim/network_simulator.hpp"

using namespace chainroute;
using namespace chainroute::sim;

int main() {
    const Node sourceNode{ChainId::Ethereum, AssetId::USDC};
    const Node destNode{ChainId::Base, AssetId::USDC};
    const double amount = 1000.0;
    const int maxTick = 50;

    for (std::uint64_t seed = 1; seed <= 2000; ++seed) {
        NetworkSimulator sim(seed);

        std::vector<std::optional<Route>> results;
        for (int t = 0; t <= maxTick; ++t) {
            const Graph g = sim.snapshot();
            const auto source = g.findNode(sourceNode);
            const auto dest = g.findNode(destNode);
            results.push_back((source && dest) ? findCheapestRoute(g, *source, *dest, amount)
                                                 : std::nullopt);
            sim.tick();
        }

        for (int t = 0; t < maxTick; ++t) {
            if (results[t].has_value() && results[t + 1].has_value()) {
                const auto& r0 = *results[t];
                const auto& r1 = *results[t + 1];
                bool sameFee = (r0.totalFee == r1.totalFee);
                bool sameEdges = r0.edges.size() == r1.edges.size();
                if (sameEdges) {
                    for (std::size_t i = 0; i < r0.edges.size(); ++i) {
                        if (r0.edges[i].bridgeName != r1.edges[i].bridgeName) {
                            sameEdges = false;
                            break;
                        }
                    }
                }
                if (!sameFee || !sameEdges) {
                    std::printf(
                        "ROUTE FLIP seed=%llu tickA=%d tickB=%d feeA=%.17g feeB=%.17g "
                        "edgesA=%zu edgesB=%zu firstBridgeA=%s firstBridgeB=%s\n",
                        static_cast<unsigned long long>(seed), t, t + 1, r0.totalFee, r1.totalFee,
                        r0.edges.size(), r1.edges.size(),
                        r0.edges.empty() ? "(none)" : r0.edges[0].bridgeName.c_str(),
                        r1.edges.empty() ? "(none)" : r1.edges[0].bridgeName.c_str());
                }
            }
        }

        for (int t = 0; t < maxTick; ++t) {
            if (results[t].has_value() && !results[t + 1].has_value()) {
                std::printf(
                    "INVALIDATION seed=%llu tickA=%d (fee=%.17g, edges=%zu) tickB=%d (nullopt)\n",
                    static_cast<unsigned long long>(seed), t, results[t]->totalFee,
                    results[t]->edges.size(), t + 1);
            }
        }
    }

    return 0;
}
```

- [ ] **Step 2: Compile and run it**

From `router/`:

```bash
g++ -std=c++20 -I include /tmp/find_fixtures.cpp \
    src/sim/deterministic_rng.cpp src/sim/network_simulator.cpp \
    src/route.cpp src/graph.cpp src/node.cpp src/edge.cpp src/chain.cpp src/asset.cpp \
    -o /tmp/find_fixtures
/tmp/find_fixtures | head -40
```

This prints candidate `ROUTE FLIP` and `INVALIDATION` lines. Pick one of
each that looks clean (a `ROUTE FLIP` line where `feeA != feeB` by more
than float noise, or the bridge sequence differs; an `INVALIDATION` line
where a route existed and then didn't). If the tool prints nothing for
one category within `seed <= 2000`, widen `maxTick` or the seed range and
re-run — this is exploratory tooling, adjust freely; it is never
committed.

For the `INVALIDATION` candidate, do one more manual check: at `tickA`,
confirm (by adding a temporary print or inspecting in a debugger/quick
script) which edge in the route has the lowest liquidity headroom above
`amount`, and confirm that specific bridge's liquidity has dropped below
`amount` by `tickB`. This confirms the transition is genuinely
liquidity-driven, not incidental.

- [ ] **Step 3: Write the pinned tests**

Using the concrete `seed`, `tickA`, `tickB`, and observed fee/edge values
from Step 2, add to `router/tests/sim/network_simulator_test.cpp`:

```cpp
TEST(NetworkSimulatorTest, CheapestRouteChangesAcrossTicks) {
    // Fixture pinned from a development-time search (Task 4, plan step 2).
    // No seed/tick search happens at test run time.
    NetworkSimulator sim(/* pinned seed */);
    const Node sourceNode{ChainId::Ethereum, AssetId::USDC};
    const Node destNode{ChainId::Base, AssetId::USDC};
    const double amount = 1000.0;

    for (int i = 0; i < /* pinned tickA */; ++i) {
        sim.tick();
    }
    const Graph gA = sim.snapshot();
    const auto sourceA = gA.findNode(sourceNode);
    const auto destA = gA.findNode(destNode);
    ASSERT_TRUE(sourceA.has_value());
    ASSERT_TRUE(destA.has_value());
    const auto routeA = findCheapestRoute(gA, *sourceA, *destA, amount);
    ASSERT_TRUE(routeA.has_value());
    EXPECT_DOUBLE_EQ(routeA->totalFee, /* observed feeA */);

    for (int i = /* pinned tickA */; i < /* pinned tickB */; ++i) {
        sim.tick();
    }
    const Graph gB = sim.snapshot();
    const auto sourceB = gB.findNode(sourceNode);
    const auto destB = gB.findNode(destNode);
    ASSERT_TRUE(sourceB.has_value());
    ASSERT_TRUE(destB.has_value());
    const auto routeB = findCheapestRoute(gB, *sourceB, *destB, amount);
    ASSERT_TRUE(routeB.has_value());
    EXPECT_DOUBLE_EQ(routeB->totalFee, /* observed feeB */);

    EXPECT_NE(routeA->totalFee, routeB->totalFee);
}

TEST(NetworkSimulatorTest, LiquidityChangeInvalidatesAPreviouslyEligibleRoute) {
    // Fixture pinned from a development-time search (Task 4, plan step 2).
    NetworkSimulator sim(/* pinned seed */);
    const Node sourceNode{ChainId::Ethereum, AssetId::USDC};
    const Node destNode{ChainId::Base, AssetId::USDC};
    const double amount = 1000.0;

    for (int i = 0; i < /* pinned tickA */; ++i) {
        sim.tick();
    }
    const Graph gA = sim.snapshot();
    const auto sourceA = gA.findNode(sourceNode);
    const auto destA = gA.findNode(destNode);
    ASSERT_TRUE(sourceA.has_value());
    ASSERT_TRUE(destA.has_value());
    const auto routeA = findCheapestRoute(gA, *sourceA, *destA, amount);
    ASSERT_TRUE(routeA.has_value());

    for (int i = /* pinned tickA */; i < /* pinned tickB */; ++i) {
        sim.tick();
    }
    const Graph gB = sim.snapshot();
    const auto sourceB = gB.findNode(sourceNode);
    const auto destB = gB.findNode(destNode);
    ASSERT_TRUE(sourceB.has_value());
    ASSERT_TRUE(destB.has_value());
    const auto routeB = findCheapestRoute(gB, *sourceB, *destB, amount);

    // Pinned expectation from the search: the route that was eligible at
    // tickA is no longer usable at tickB (either no route, or a route
    // that no longer includes the same first bridge).
    EXPECT_FALSE(routeB.has_value());
}
```

Fill in every `/* pinned ... */` placeholder with the literal values
observed in Step 2. If the found `INVALIDATION` example results in a
*different* route at `tickB` rather than `nullopt`, change the final
assertion to check that instead (e.g.
`EXPECT_NE(routeA->edges[0].bridgeName, routeB->edges[0].bridgeName);`
plus `ASSERT_TRUE(routeB.has_value())`) — match the assertion to what was
actually observed.

- [ ] **Step 4: Build and run, verify both tests pass**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=NetworkSimulatorTest.*
```

Expected: `PASSED` (11 tests total).

- [ ] **Step 5: Delete the scratch search tool**

```bash
rm -f /tmp/find_fixtures.cpp /tmp/find_fixtures /tmp/print_rng_golden.cpp /tmp/print_rng_golden
```

Confirm `git status` shows no untracked files from this search process.

- [ ] **Step 6: Commit**

```bash
git add router/tests/sim/network_simulator_test.cpp
git commit -m "test(router): pin route-flip and liquidity-invalidation fixtures"
```

---

### Task 5: Full build verification, spec-compliance review, and reporting

**Files:**
- None created. Potentially modify any file if a warning or failure
  surfaces.

**Interfaces:**
- Consumes: the complete `chainroute` library (Phase 1 + 2 + 3) and
  `chainroute_tests` binary from Tasks 1-4.
- Produces: a clean, warning-free build; the full test suite passing; a
  spec-compliance review; confirmation Phase 1/2 files are byte-identical
  to before this plan; a files-changed listing; a final architecture
  summary; and one concrete route-flip example, all reported to the user.

- [ ] **Step 1: Clean configure and build from scratch, with warnings enabled**

Run:

```bash
rm -rf router/build
cmake -S router -B router/build
cmake --build router/build 2>&1 | tee /tmp/chainroute_phase3_build.log
```

- [ ] **Step 2: Inspect the build log for warnings in project code**

Run:

```bash
grep -E "router/(src|include)/" /tmp/chainroute_phase3_build.log || echo "No project-code warnings"
```

If any warning appears in `router/src/` or `router/include/`, fix the
underlying code — do not suppress with pragmas. Rebuild until clean.

- [ ] **Step 3: Run the complete Phase 1 + 2 + 3 test suite**

Run:

```bash
ctest --test-dir router/build --output-on-failure
```

Expected: all 43 tests pass (22 Phase 1, 10 Phase 2, 11 Phase 3). If any
test fails, use systematic-debugging to find the root cause in the
implementation (not the test) unless the test itself is proven wrong,
then fix and re-run until green.

- [ ] **Step 4: Verify Phase 1/2 source was not changed**

Run:

```bash
git diff --stat 4bfa6b3..HEAD -- router/include/chainroute/graph.hpp router/include/chainroute/node.hpp router/include/chainroute/edge.hpp router/include/chainroute/route.hpp router/include/chainroute/chain.hpp router/include/chainroute/asset.hpp router/src/graph.cpp router/src/node.cpp router/src/edge.cpp router/src/route.cpp router/src/chain.cpp router/src/asset.cpp
```

(If `4bfa6b3` is not the right pre-Phase-3 commit in this checkout, use
`git log --oneline -- router/` to find the last Phase 2 commit and diff
from there instead.) Expected: empty output — zero changes to any Phase
1/2 file. If this shows any changes, that is a plan violation; investigate
and revert unless a demonstrated correctness issue justifies it, in which
case stop and report to the human partner rather than proceeding.

- [ ] **Step 5: Review the implementation against the Phase 3 spec**

Read `router/src/sim/network_simulator.cpp` and
`router/src/sim/deterministic_rng.cpp` fresh, alongside
`docs/superpowers/specs/2026-09-10-cpp-network-simulator-design.md`, and
confirm:
- No `std::hash<std::string>`, no `<random>` distribution, no
  `unordered_map`/`unordered_set` used anywhere under `router/*/sim/`
  (`grep -rn "uniform_real_distribution\|unordered_map\|unordered_set" router/src/sim router/include/chainroute/sim` — expect no matches, or only the test file's `unordered_map` used for a local counting scan, which is fine since it doesn't affect determinism).
- `NetworkSimulator`'s only mutable state is `tick_` (read the class —
  `seed_` and `topology_` are never reassigned after construction).
- `snapshot()` never returns a reference or pointer into `this` — it
  returns `Graph` by value, constructed fresh each call.
- Bridge inclusion is decided independently per ordered `(source, target)`
  pair — confirm there is no code path that derives one direction's
  inclusion from the other's.
- Every RNG `role` string is a literal, never built via concatenation.
- `router/src/route.cpp` and `router/src/graph.cpp` are not included by
  anything under `router/*/sim/` except `route.hpp`/`graph.hpp` (a normal
  dependency on the Phase 1/2 *public API*, not a modification).

If this review finds a real defect, fix it, add a regression test, and
re-run the full suite before proceeding.

- [ ] **Step 6: List files changed**

Run:

```bash
git diff --stat 4bfa6b3..HEAD -- router/
```

- [ ] **Step 7: Prepare the final report for the user**

Write out, for the response to the user:
- The final `NetworkSimulator` architecture, summarized conceptually:
  what state it owns, how `snapshot()` builds a `Graph`, how determinism
  is achieved, how topology generation works.
- The files-changed listing from Step 6.
- Confirmation that Phase 1/2 source is unchanged (Step 4's empty diff).
- Test summary (43/43, 0 project warnings).
- **One concrete example of the cheapest route changing between ticks**:
  use the exact pinned seed/tickA/tickB/fee/bridge values from Task 4's
  `CheapestRouteChangesAcrossTicks` test to describe the specific
  transition (e.g. "at tick N, Ethereum-USDC → Base-USDC costs $X via
  bridge Y; at tick M, the cheapest route costs $X′ via bridge Y′").

This becomes the response to the user's requests for files changed,
architecture summary, and a concrete route-flip example — write it out
fully; do not defer it.

- [ ] **Step 8: Commit (only if Step 2 or Step 5 required code changes)**

```bash
git add -A
git commit -m "fix(router): resolve issues found during Phase 3 verification"
```

If no changes were needed, skip this commit.
