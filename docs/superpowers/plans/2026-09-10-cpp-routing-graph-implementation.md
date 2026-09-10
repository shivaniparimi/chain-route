# ChainRoute Phase 1 C++ Routing Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Node/Edge/Graph data model described in `docs/superpowers/specs/2026-09-10-cpp-routing-graph-design.md`, with a full GoogleTest suite, building cleanly under CMake with C++20.

**Architecture:** A small static library (`chainroute`) under `router/`, split into one header/source pair per type (`chain`, `asset`, `node`, `edge`, `graph`), all in namespace `chainroute`. `Graph` stores nodes in an append-only vector with a hash-map index for dedup, and edges in a parallel vector-of-vectors adjacency list keyed by integer `NodeIndex`.

**Tech Stack:** C++20, CMake (>= 3.20), GoogleTest via CMake `FetchContent`.

## Global Constraints

- Language standard: C++20, enforced via `CMAKE_CXX_STANDARD 20` / `CMAKE_CXX_STANDARD_REQUIRED ON`.
- Build system: CMake only (no other build tool).
- Everything lives in namespace `chainroute`.
- `Graph` is an index-based adjacency list (`std::vector<std::vector<Edge>>`), never an adjacency matrix.
- GoogleTest is fetched via CMake `FetchContent` — no manually-installed dependency.
- In scope: `Node`, `Edge`, `Graph`, and their `toString` helpers only.
- Out of scope: any routing/shortest-path algorithm, persistence, networking, blockchain API integration, or anything else beyond the approved spec.
- Every `NodeIndex` argument that indexes into `Graph` internals (`nodeAt`, `edgesFrom`, `addEdge`'s `from` and `Edge::to`) must be validated and throw `std::out_of_range` when invalid.
- `Graph` must support parallel edges (multiple edges between the same ordered pair of nodes).

---

### Task 1: Project scaffolding + ChainId

**Files:**
- Create: `router/CMakeLists.txt`
- Create: `router/tests/CMakeLists.txt`
- Create: `router/include/chainroute/chain.hpp`
- Create: `router/src/chain.cpp`
- Test: `router/tests/node_test.cpp`

**Interfaces:**
- Produces: `namespace chainroute { enum class ChainId : uint8_t { Ethereum, Base, Arbitrum, Optimism, Polygon }; std::string toString(ChainId chain); }`

- [ ] **Step 1: Create the top-level router CMakeLists.txt**

`router/CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(chainroute_router CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(chainroute STATIC
    src/chain.cpp
)
target_include_directories(chainroute PUBLIC include)

if (NOT MSVC)
    target_compile_options(chainroute PUBLIC -Wall -Wextra -Wpedantic)
endif()

enable_testing()
add_subdirectory(tests)
```

- [ ] **Step 2: Create the tests CMakeLists.txt with GoogleTest via FetchContent**

`router/tests/CMakeLists.txt`:

```cmake
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(chainroute_tests
    node_test.cpp
)
target_link_libraries(chainroute_tests PRIVATE chainroute GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(chainroute_tests)
```

- [ ] **Step 3: Write the failing test for ChainId::toString**

`router/tests/node_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include "chainroute/chain.hpp"

namespace chainroute {
namespace {

TEST(ChainIdTest, ToStringKnownValues) {
    EXPECT_EQ(toString(ChainId::Ethereum), "Ethereum");
    EXPECT_EQ(toString(ChainId::Base), "Base");
    EXPECT_EQ(toString(ChainId::Arbitrum), "Arbitrum");
    EXPECT_EQ(toString(ChainId::Optimism), "Optimism");
    EXPECT_EQ(toString(ChainId::Polygon), "Polygon");
}

}  // namespace
}  // namespace chainroute
```

- [ ] **Step 4: Create the (currently declaration-only) chain.hpp so the test fails to link, not to compile**

`router/include/chainroute/chain.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <string>

namespace chainroute {

enum class ChainId : uint8_t {
    Ethereum,
    Base,
    Arbitrum,
    Optimism,
    Polygon,
};

std::string toString(ChainId chain);

}  // namespace chainroute
```

Also create an empty `router/src/chain.cpp` placeholder containing only `#include "chainroute/chain.hpp"` for this step, so the project configures.

- [ ] **Step 5: Configure and build, verify the test fails**

Run:

```bash
cd router
cmake -S . -B build
cmake --build build
```

Expected: build fails with an undefined-reference linker error for `chainroute::toString(chainroute::ChainId)` (the test compiles against the declaration but there is no definition yet).

- [ ] **Step 6: Implement toString(ChainId)**

`router/src/chain.cpp`:

```cpp
#include "chainroute/chain.hpp"

namespace chainroute {

std::string toString(ChainId chain) {
    switch (chain) {
        case ChainId::Ethereum: return "Ethereum";
        case ChainId::Base: return "Base";
        case ChainId::Arbitrum: return "Arbitrum";
        case ChainId::Optimism: return "Optimism";
        case ChainId::Polygon: return "Polygon";
    }
    return "Unknown";
}

}  // namespace chainroute
```

- [ ] **Step 7: Build and run, verify the test passes**

Run:

```bash
cmake --build build
./build/tests/chainroute_tests --gtest_filter=ChainIdTest.*
```

Expected: `PASSED`.

- [ ] **Step 8: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/chain.hpp router/src/chain.cpp router/tests/node_test.cpp
git commit -m "feat(router): add ChainId enum with toString and project scaffolding"
```

---

### Task 2: AssetId

**Files:**
- Create: `router/include/chainroute/asset.hpp`
- Create: `router/src/asset.cpp`
- Modify: `router/CMakeLists.txt` (add `src/asset.cpp` to the `chainroute` library sources)
- Modify: `router/tests/node_test.cpp` (add AssetId tests)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `namespace chainroute { enum class AssetId : uint8_t { USDC, USDT, ETH, WBTC, DAI }; std::string toString(AssetId asset); }`

- [ ] **Step 1: Write the failing test for AssetId::toString**

Add to `router/tests/node_test.cpp` (inside the existing `namespace chainroute { namespace { ... } }` block, alongside `ChainIdTest`):

```cpp
TEST(AssetIdTest, ToStringKnownValues) {
    EXPECT_EQ(toString(AssetId::USDC), "USDC");
    EXPECT_EQ(toString(AssetId::USDT), "USDT");
    EXPECT_EQ(toString(AssetId::ETH), "ETH");
    EXPECT_EQ(toString(AssetId::WBTC), "WBTC");
    EXPECT_EQ(toString(AssetId::DAI), "DAI");
}
```

Add `#include "chainroute/asset.hpp"` to the top of `router/tests/node_test.cpp`.

- [ ] **Step 2: Create asset.hpp**

`router/include/chainroute/asset.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <string>

namespace chainroute {

enum class AssetId : uint8_t {
    USDC,
    USDT,
    ETH,
    WBTC,
    DAI,
};

std::string toString(AssetId asset);

}  // namespace chainroute
```

Create `router/src/asset.cpp` containing only `#include "chainroute/asset.hpp"` for now.

- [ ] **Step 3: Add asset.cpp to the library and verify the test fails to link**

In `router/CMakeLists.txt`, change:

```cmake
add_library(chainroute STATIC
    src/chain.cpp
)
```

to:

```cmake
add_library(chainroute STATIC
    src/chain.cpp
    src/asset.cpp
)
```

Run:

```bash
cmake -S . -B build
cmake --build build
```

Expected: undefined-reference linker error for `chainroute::toString(chainroute::AssetId)`.

- [ ] **Step 4: Implement toString(AssetId)**

`router/src/asset.cpp`:

```cpp
#include "chainroute/asset.hpp"

namespace chainroute {

std::string toString(AssetId asset) {
    switch (asset) {
        case AssetId::USDC: return "USDC";
        case AssetId::USDT: return "USDT";
        case AssetId::ETH: return "ETH";
        case AssetId::WBTC: return "WBTC";
        case AssetId::DAI: return "DAI";
    }
    return "Unknown";
}

}  // namespace chainroute
```

- [ ] **Step 5: Build and run, verify the test passes**

Run:

```bash
cmake --build build
./build/tests/chainroute_tests --gtest_filter=AssetIdTest.*
```

Expected: `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add router/CMakeLists.txt router/include/chainroute/asset.hpp router/src/asset.cpp router/tests/node_test.cpp
git commit -m "feat(router): add AssetId enum with toString"
```

---

### Task 3: Node

**Files:**
- Create: `router/include/chainroute/node.hpp`
- Create: `router/src/node.cpp`
- Modify: `router/CMakeLists.txt` (add `src/node.cpp`)
- Modify: `router/tests/node_test.cpp` (add Node tests)

**Interfaces:**
- Consumes: `ChainId`, `toString(ChainId)` (Task 1); `AssetId`, `toString(AssetId)` (Task 2).
- Produces:
```cpp
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
}
```

- [ ] **Step 1: Write the failing tests for Node**

Add to `router/tests/node_test.cpp`:

```cpp
TEST(NodeTest, EqualNodesCompareEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(a, b);
}

TEST(NodeTest, DifferentChainCompareNotEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Base, AssetId::USDC};
    EXPECT_NE(a, b);
}

TEST(NodeTest, DifferentAssetCompareNotEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDT};
    EXPECT_NE(a, b);
}

TEST(NodeTest, EqualNodesHashEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(NodeHash{}(a), NodeHash{}(b));
}

TEST(NodeTest, HashUsableInUnorderedSet) {
    std::unordered_set<Node, NodeHash> nodes;
    nodes.insert(Node{ChainId::Ethereum, AssetId::USDC});
    nodes.insert(Node{ChainId::Ethereum, AssetId::USDC});
    nodes.insert(Node{ChainId::Base, AssetId::USDC});
    EXPECT_EQ(nodes.size(), 2u);
}

TEST(NodeTest, ToStringFormatsChainDashAsset) {
    const Node node{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(toString(node), "Ethereum-USDC");
}
```

Add `#include "chainroute/node.hpp"` and `#include <unordered_set>` to the top of `router/tests/node_test.cpp`.

- [ ] **Step 2: Create node.hpp**

`router/include/chainroute/node.hpp`:

```cpp
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
```

Create `router/src/node.cpp` containing only `#include "chainroute/node.hpp"` for now.

- [ ] **Step 3: Add node.cpp to the library and verify the tests fail to link**

In `router/CMakeLists.txt`, add `src/node.cpp` to the `add_library(chainroute STATIC ...)` source list (alongside `chain.cpp` and `asset.cpp`).

Run:

```bash
cmake -S . -B build
cmake --build build
```

Expected: undefined-reference linker errors for `NodeHash::operator()` and `toString(const Node&)`.

- [ ] **Step 4: Implement Node's out-of-line members**

`router/src/node.cpp`:

```cpp
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
```

- [ ] **Step 5: Build and run, verify the tests pass**

Run:

```bash
cmake --build build
./build/tests/chainroute_tests --gtest_filter=NodeTest.*
```

Expected: `PASSED` (5 tests).

- [ ] **Step 6: Commit**

```bash
git add router/CMakeLists.txt router/include/chainroute/node.hpp router/src/node.cpp router/tests/node_test.cpp
git commit -m "feat(router): add Node struct with equality, hashing, and toString"
```

---

### Task 4: Edge

**Files:**
- Create: `router/include/chainroute/edge.hpp`
- Create: `router/src/edge.cpp`
- Create: `router/tests/edge_test.cpp`
- Modify: `router/CMakeLists.txt` (add `src/edge.cpp`)
- Modify: `router/tests/CMakeLists.txt` (add `edge_test.cpp` to `chainroute_tests` sources)

**Interfaces:**
- Consumes: nothing from Tasks 1-3.
- Produces:
```cpp
namespace chainroute {
using NodeIndex = std::size_t;
struct Edge {
    NodeIndex to;
    std::string bridgeName;
    double fee;
    double latencyMs;
    double liquidity;
    double reliability;
};
}
```

- [ ] **Step 1: Write the failing test for Edge**

`router/tests/edge_test.cpp`:

```cpp
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
```

- [ ] **Step 2: Register edge_test.cpp in the test executable**

In `router/tests/CMakeLists.txt`, change:

```cmake
add_executable(chainroute_tests
    node_test.cpp
)
```

to:

```cmake
add_executable(chainroute_tests
    node_test.cpp
    edge_test.cpp
)
```

- [ ] **Step 3: Configure and build, verify it fails (missing header)**

Run:

```bash
cmake -S . -B build
cmake --build build
```

Expected: compile failure — `chainroute/edge.hpp` not found.

- [ ] **Step 4: Create edge.hpp and edge.cpp**

`router/include/chainroute/edge.hpp`:

```cpp
#pragma once

#include <cstddef>
#include <string>

namespace chainroute {

using NodeIndex = std::size_t;

struct Edge {
    NodeIndex to;
    std::string bridgeName;
    double fee;
    double latencyMs;
    double liquidity;
    double reliability;
};

}  // namespace chainroute
```

`router/src/edge.cpp`:

```cpp
#include "chainroute/edge.hpp"
```

In `router/CMakeLists.txt`, add `src/edge.cpp` to the `add_library(chainroute STATIC ...)` source list.

- [ ] **Step 5: Build and run, verify the test passes**

Run:

```bash
cmake -S . -B build
cmake --build build
./build/tests/chainroute_tests --gtest_filter=EdgeTest.*
```

Expected: `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/edge.hpp router/src/edge.cpp router/tests/edge_test.cpp
git commit -m "feat(router): add Edge struct with NodeIndex alias"
```

---

### Task 5: Graph — node management (addNode, findNode, nodeAt, nodeCount)

**Files:**
- Create: `router/include/chainroute/graph.hpp`
- Create: `router/src/graph.cpp`
- Create: `router/tests/graph_test.cpp`
- Modify: `router/CMakeLists.txt` (add `src/graph.cpp`)
- Modify: `router/tests/CMakeLists.txt` (add `graph_test.cpp`)

**Interfaces:**
- Consumes: `Node`, `NodeHash` (Task 3); `Edge`, `NodeIndex` (Task 4).
- Produces (full class declared now; `addEdge`/`edgesFrom`/`edgeCount` implemented in Task 6):
```cpp
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
}
```
`nodeAt` throws `std::out_of_range` for an index `>= nodeCount()`.

- [ ] **Step 1: Write the failing tests for node management**

`router/tests/graph_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include <stdexcept>

#include "chainroute/graph.hpp"

namespace chainroute {
namespace {

TEST(GraphTest, AddNodeIsIdempotentForSameNode) {
    Graph graph;
    const NodeIndex first = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex second = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(first, second);
    EXPECT_EQ(graph.nodeCount(), 1u);
}

TEST(GraphTest, AddNodeAssignsDistinctIndicesForDistinctNodes) {
    Graph graph;
    const NodeIndex a = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex b = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_NE(a, b);
    EXPECT_EQ(graph.nodeCount(), 2u);
}

TEST(GraphTest, FindNodeReturnsIndexWhenPresent) {
    Graph graph;
    const NodeIndex index = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const auto found = graph.findNode(Node{ChainId::Ethereum, AssetId::USDC});
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, index);
}

TEST(GraphTest, FindNodeReturnsNulloptWhenAbsent) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const auto found = graph.findNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_FALSE(found.has_value());
}

TEST(GraphTest, NodeAtReturnsCorrectNode) {
    Graph graph;
    const NodeIndex index = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(graph.nodeAt(index), (Node{ChainId::Ethereum, AssetId::USDC}));
}

TEST(GraphTest, NodeAtThrowsOnInvalidIndex) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(graph.nodeAt(42), std::out_of_range);
}

TEST(GraphTest, NodeCountReflectsDistinctNodesAdded) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    graph.addNode(Node{ChainId::Base, AssetId::USDC});
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(graph.nodeCount(), 2u);
}

}  // namespace
}  // namespace chainroute
```

- [ ] **Step 2: Register graph_test.cpp and graph.cpp**

In `router/tests/CMakeLists.txt`, add `graph_test.cpp` to `add_executable(chainroute_tests ...)`.

In `router/CMakeLists.txt`, add `src/graph.cpp` to `add_library(chainroute STATIC ...)`.

- [ ] **Step 3: Create graph.hpp with the full class declaration**

`router/include/chainroute/graph.hpp`:

```cpp
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
```

`router/src/graph.cpp` (only node-management members implemented in this task):

```cpp
#include "chainroute/graph.hpp"

#include <stdexcept>

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

}  // namespace chainroute
```

Note: `addEdge`, `edgesFrom`, and `edgeCount` are declared in the header but not yet defined here — that is fine, since this task's tests never call them, so the linker never needs their definitions.

- [ ] **Step 4: Build and run, verify the tests pass**

Run:

```bash
cmake -S . -B build
cmake --build build
./build/tests/chainroute_tests --gtest_filter=GraphTest.*
```

Expected: `PASSED` (7 tests).

- [ ] **Step 5: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/graph.hpp router/src/graph.cpp router/tests/graph_test.cpp
git commit -m "feat(router): add Graph node management (addNode, findNode, nodeAt, nodeCount)"
```

---

### Task 6: Graph — edge management (addEdge, edgesFrom, edgeCount)

**Files:**
- Modify: `router/src/graph.cpp` (add `addEdge`, `edgesFrom`, `edgeCount`)
- Modify: `router/tests/graph_test.cpp` (add edge-management tests)

**Interfaces:**
- Consumes: `Graph` class declared in Task 5 (no header changes needed — all three methods are already declared).
- Produces: working `addEdge`, `edgesFrom`, `edgeCount` bodies. `addEdge` throws `std::out_of_range` if `from` or `edge.to` is `>= nodeCount()`. `edgesFrom` throws `std::out_of_range` if `idx >= nodeCount()`.

- [ ] **Step 1: Write the failing tests for edge management**

Add to `router/tests/graph_test.cpp` (inside the existing anonymous namespace, alongside the Task 5 tests):

```cpp
TEST(GraphTest, AddEdgeCreatesDirectedEdge) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});

    EXPECT_EQ(graph.edgesFrom(ethUsdc).size(), 1u);
    EXPECT_TRUE(graph.edgesFrom(baseUsdc).empty());
}

TEST(GraphTest, AddEdgeSupportsParallelEdgesBetweenSamePair) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{baseUsdc, "Wormhole", 0.5, 1500.0, 50000.0, 0.95});

    const auto& edges = graph.edgesFrom(ethUsdc);
    ASSERT_EQ(edges.size(), 2u);
    EXPECT_EQ(edges[0].bridgeName, "Stargate");
    EXPECT_EQ(edges[1].bridgeName, "Wormhole");
}

TEST(GraphTest, EdgesFromThrowsOnInvalidIndex) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(graph.edgesFrom(42), std::out_of_range);
}

TEST(GraphTest, AddEdgeThrowsWhenFromIndexInvalid) {
    Graph graph;
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_THROW(
        graph.addEdge(42, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99}),
        std::out_of_range);
}

TEST(GraphTest, AddEdgeThrowsWhenToIndexInvalid) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(
        graph.addEdge(ethUsdc, Edge{42, "Stargate", 1.0, 500.0, 100000.0, 0.99}),
        std::out_of_range);
}

TEST(GraphTest, EdgeCountReflectsAllEdgesAcrossNodes) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "Wormhole", 0.8, 900.0, 80000.0, 0.97});
    graph.addEdge(baseUsdc, Edge{arbUsdc, "Across", 0.3, 300.0, 60000.0, 0.98});

    EXPECT_EQ(graph.edgeCount(), 3u);
}
```

- [ ] **Step 2: Build, verify the new tests fail to link**

Run:

```bash
cmake --build build
```

Expected: undefined-reference linker errors for `Graph::addEdge`, `Graph::edgesFrom`, `Graph::edgeCount`.

- [ ] **Step 3: Implement addEdge, edgesFrom, edgeCount**

Add to `router/src/graph.cpp` (after the Task 5 members, before the closing `}  // namespace chainroute`):

```cpp
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
```

Add `#include <utility>` to the top of `router/src/graph.cpp` for `std::move`.

- [ ] **Step 4: Build and run, verify all tests pass**

Run:

```bash
cmake --build build
./build/tests/chainroute_tests --gtest_filter=GraphTest.*
```

Expected: `PASSED` (13 tests).

- [ ] **Step 5: Commit**

```bash
git add router/src/graph.cpp router/tests/graph_test.cpp
git commit -m "feat(router): add Graph edge management (addEdge, edgesFrom, edgeCount)"
```

---

### Task 7: Full build verification and cleanup

**Files:**
- None created. Potentially modify any file if a warning or failure surfaces.

**Interfaces:**
- Consumes: the complete `chainroute` library and `chainroute_tests` binary from Tasks 1-6.
- Produces: a clean, warning-free build and a fully passing test suite; the final directory tree and an implementation summary for the user.

- [ ] **Step 1: Clean configure and build from scratch**

Run:

```bash
cd router
rm -rf build
cmake -S . -B build
cmake --build build 2>&1 | tee /tmp/chainroute_build.log
```

- [ ] **Step 2: Inspect the build log for warnings**

Run:

```bash
grep -i "warning" /tmp/chainroute_build.log || echo "No warnings"
```

If any warnings appear (e.g. unused variable, sign comparison, missing switch case), fix the underlying code in the relevant `router/src/*.cpp` or `router/include/chainroute/*.hpp` file — do not suppress with pragmas — and rebuild until the log is clean.

- [ ] **Step 3: Run the complete test suite**

Run:

```bash
ctest --test-dir build --output-on-failure
```

Expected: all tests pass (26 assertions across `ChainIdTest`, `AssetIdTest`, `NodeTest`, `EdgeTest`, `GraphTest`). If any test fails, use systematic-debugging to find the root cause in the implementation (not the test) unless the test itself is proven wrong, then fix and re-run until green.

- [ ] **Step 4: Print the final directory structure**

Run:

```bash
find router -type f -not -path "*/build/*" | sort
```

- [ ] **Step 5: Summarize what was implemented for the user**

Report: the files created, the public API of `Node`/`Edge`/`Graph`, the total test count and pass/fail status, and an explicit note that no routing algorithm was implemented (Phase 2 remains untouched).

- [ ] **Step 6: Commit (only if Step 2 or Step 3 required code changes)**

```bash
git add -A
git commit -m "fix(router): resolve build warnings / test failures found during verification"
```

If no changes were needed in Steps 2-3, skip this commit — there is nothing new to record.
