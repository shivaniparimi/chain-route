# ChainRoute Phase 2 Dijkstra Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `findCheapestRoute`, the fee-only Dijkstra router described in `docs/superpowers/specs/2026-09-10-cpp-dijkstra-routing-design.md`, with a full GoogleTest suite covering the worked example and every listed edge case, building cleanly under the existing Phase 1 CMake project.

**Architecture:** One new header/source pair (`route.hpp`/`route.cpp`) added to the existing `chainroute` static library under `router/`, plus one new test file (`route_test.cpp`). Binary-heap Dijkstra with lazy deletion runs directly against `Graph::edgesFrom`, storing per-node predecessor node *and* predecessor edge (since `Edge` only stores `to`, not `from`) so the exact chosen `Edge` sequence — including which specific parallel edge was used — can be reconstructed.

**Tech Stack:** C++20, existing CMake project, GoogleTest (already wired via FetchContent in `router/tests/CMakeLists.txt`).

## Global Constraints

- Language standard: C++20 (already set project-wide in `router/CMakeLists.txt`; no changes needed).
- Build system: CMake only, extending the existing `router/CMakeLists.txt` / `router/tests/CMakeLists.txt`.
- Everything lives in namespace `chainroute`.
- Do NOT modify `Graph`, `Node`, or `Edge` (in `router/include/chainroute/graph.hpp`, `node.hpp`, `edge.hpp`) unless a demonstrated correctness issue requires it. None is expected in this plan.
- Routing objective is fee-only: minimize the sum of `Edge::fee` along the route. Do NOT incorporate `latencyMs` or `reliability` into the cost in any way.
- Liquidity is a hard eligibility constraint: an edge is a candidate only if `edge.liquidity >= amount`; it is never used otherwise, regardless of cost.
- `Route` is exactly `{ std::vector<Edge> edges; double totalFee; }` — no separate node-index path field.
- `findCheapestRoute(const Graph& graph, NodeIndex source, NodeIndex destination, double amount)` returns `std::optional<Route>`: `std::nullopt` means no eligible route exists (normal outcome, not an error).
- `source`/`destination` `>= graph.nodeCount()` throws `std::out_of_range`, matching `Graph`'s own validation convention.
- `source == destination` is valid and returns `Route{{}, 0.0}`.
- Complexity target: O((V + E) log E) time, O(V + E) auxiliary space, where V = `graph.nodeCount()`, E = `graph.edgeCount()` — do not assume E is bounded by V² (Graph is a multigraph with unbounded parallel edges).
- Out of scope for this plan: any Go, Kafka, Redis, database, networking, or blockchain API code; any Phase 3 functionality (alternate objectives, multi-criteria routing, a pluggable cost-function abstraction).
- Build with warnings enabled (`-Wall -Wextra -Wpedantic`, already configured `PRIVATE` on the `chainroute` target) and keep the build warning-free for project code.

---

### Task 1: Route struct, project wiring, and core Dijkstra correctness (multi-hop cheaper than direct)

**Files:**
- Create: `router/include/chainroute/route.hpp`
- Create: `router/src/route.cpp`
- Create: `router/tests/route_test.cpp`
- Modify: `router/CMakeLists.txt` (add `src/route.cpp` to the `chainroute` library sources)
- Modify: `router/tests/CMakeLists.txt` (add `route_test.cpp` to `chainroute_tests` sources)

**Interfaces:**
- Consumes: `Graph`, `NodeIndex`, `Edge` (all from Phase 1, unchanged) — `Graph::nodeCount()`, `Graph::edgesFrom(NodeIndex) const -> const std::vector<Edge>&`.
- Produces (this is the full, final public interface — later tasks add tests only, no header/signature changes):
```cpp
namespace chainroute {
struct Route {
    std::vector<Edge> edges;
    double totalFee;
};
std::optional<Route> findCheapestRoute(
    const Graph& graph, NodeIndex source, NodeIndex destination, double amount);
}
```

- [ ] **Step 1: Write the failing test — multi-hop cheaper than direct (the worked example)**

`router/tests/route_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include <stdexcept>

#include "chainroute/route.hpp"

namespace chainroute {
namespace {

TEST(RouteTest, PrefersCheaperMultiHopOverDirectRoute) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "HopBridge1", 1.0, 500.0, 1000000.0, 0.99});
    graph.addEdge(arbUsdc, Edge{baseUsdc, "HopBridge2", 1.5, 500.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 2.5);
    ASSERT_EQ(route->edges.size(), 2u);
    EXPECT_EQ(route->edges[0].bridgeName, "HopBridge1");
    EXPECT_EQ(route->edges[1].bridgeName, "HopBridge2");
}

}  // namespace
}  // namespace chainroute
```

- [ ] **Step 2: Register the new files in CMake**

In `router/CMakeLists.txt`, add `src/route.cpp` to the `add_library(chainroute STATIC ...)` source list (alongside `chain.cpp`, `asset.cpp`, `node.cpp`, `edge.cpp`, `graph.cpp`).

In `router/tests/CMakeLists.txt`, add `route_test.cpp` to the `add_executable(chainroute_tests ...)` source list (alongside `node_test.cpp`, `edge_test.cpp`, `graph_test.cpp`).

- [ ] **Step 3: Create route.hpp (declaration only) and a stub route.cpp, verify the test fails to link**

`router/include/chainroute/route.hpp`:

```cpp
#pragma once

#include <optional>
#include <vector>

#include "chainroute/edge.hpp"
#include "chainroute/graph.hpp"

namespace chainroute {

struct Route {
    std::vector<Edge> edges;
    double totalFee;
};

std::optional<Route> findCheapestRoute(
    const Graph& graph, NodeIndex source, NodeIndex destination, double amount);

}  // namespace chainroute
```

`router/src/route.cpp` (stub, no real logic yet):

```cpp
#include "chainroute/route.hpp"
```

Run:

```bash
cmake -S router -B router/build
cmake --build router/build
```

Expected: undefined-reference linker error for `chainroute::findCheapestRoute`.

- [ ] **Step 4: Implement the full Dijkstra algorithm**

`router/src/route.cpp`:

```cpp
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
```

- [ ] **Step 5: Build and run, verify the test passes**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=RouteTest.*
```

Expected: `PASSED` (1 test).

- [ ] **Step 6: Commit**

```bash
git add router/CMakeLists.txt router/tests/CMakeLists.txt router/include/chainroute/route.hpp router/src/route.cpp router/tests/route_test.cpp
git commit -m "feat(router): add findCheapestRoute with core Dijkstra correctness test"
```

---

### Task 2: Liquidity hard-constraint tests

**Files:**
- Modify: `router/tests/route_test.cpp` (add 3 tests; no production code changes expected)

**Interfaces:**
- Consumes: `findCheapestRoute` as implemented in Task 1 (no signature changes).

- [ ] **Step 1: Write the failing/passing tests for liquidity as a hard constraint**

Add to `router/tests/route_test.cpp` (inside the existing anonymous namespace, alongside `PrefersCheaperMultiHopOverDirectRoute`):

```cpp
TEST(RouteTest, FallsBackWhenCheapestPathFailsLiquidity) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "HopBridge1", 1.0, 500.0, 500.0, 0.99});
    graph.addEdge(arbUsdc, Edge{baseUsdc, "HopBridge2", 1.5, 500.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 4.0);
    ASSERT_EQ(route->edges.size(), 1u);
    EXPECT_EQ(route->edges[0].bridgeName, "DirectBridge");
}

TEST(RouteTest, ReturnsNulloptWhenLiquidityEliminatesAllPaths) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "SmallBridge", 4.0, 1000.0, 500.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}

TEST(RouteTest, RejectsMultiHopWhenAnIntermediateEdgeLacksLiquidity) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});
    const NodeIndex polyUsdc = graph.addNode(Node{ChainId::Polygon, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    // Cheapest-looking 3-hop chain: Ethereum -> Arbitrum -> Polygon -> Base.
    // The middle leg (Arbitrum -> Polygon) doesn't have enough liquidity for the payment.
    graph.addEdge(ethUsdc, Edge{arbUsdc, "Hop1", 0.5, 500.0, 1000000.0, 0.99});
    graph.addEdge(arbUsdc, Edge{polyUsdc, "Hop2", 0.5, 500.0, 500.0, 0.99});
    graph.addEdge(polyUsdc, Edge{baseUsdc, "Hop3", 0.5, 500.0, 1000000.0, 0.99});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 4.0);
    ASSERT_EQ(route->edges.size(), 1u);
    EXPECT_EQ(route->edges[0].bridgeName, "DirectBridge");
}
```

- [ ] **Step 2: Build and run, verify all three tests pass**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=RouteTest.*
```

Expected: `PASSED` (4 tests: the Task 1 test plus these 3). If any fails, the liquidity-skip condition in `route.cpp`'s relaxation loop (`if (edge.liquidity < amount) continue;`) is the first place to check — do not weaken a test to make it pass.

- [ ] **Step 3: Commit**

```bash
git add router/tests/route_test.cpp
git commit -m "test(router): cover liquidity as a hard eligibility constraint"
```

---

### Task 3: Graph-shape edge cases (disconnected, same-node, isolated destination)

**Files:**
- Modify: `router/tests/route_test.cpp` (add 3 tests; no production code changes expected)

**Interfaces:**
- Consumes: `findCheapestRoute` as implemented in Task 1 (no signature changes).

- [ ] **Step 1: Write the tests**

Add to `router/tests/route_test.cpp`:

```cpp
TEST(RouteTest, ReturnsNulloptWhenGraphIsDisconnected) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}

TEST(RouteTest, ReturnsTrivialRouteWhenSourceEqualsDestination) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});

    const auto route = findCheapestRoute(graph, ethUsdc, ethUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_TRUE(route->edges.empty());
    EXPECT_DOUBLE_EQ(route->totalFee, 0.0);
}

TEST(RouteTest, ReturnsNulloptForIsolatedDestination) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex isolatedUsdc = graph.addNode(Node{ChainId::Polygon, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, isolatedUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}
```

- [ ] **Step 2: Build and run, verify all tests pass**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=RouteTest.*
```

Expected: `PASSED` (7 tests total).

- [ ] **Step 3: Commit**

```bash
git add router/tests/route_test.cpp
git commit -m "test(router): cover disconnected, same-node, and isolated-destination routes"
```

---

### Task 4: Parallel edges and invalid-index tests

**Files:**
- Modify: `router/tests/route_test.cpp` (add 3 tests; no production code changes expected)

**Interfaces:**
- Consumes: `findCheapestRoute` as implemented in Task 1 (no signature changes).

- [ ] **Step 1: Write the tests**

Add to `router/tests/route_test.cpp`:

```cpp
TEST(RouteTest, PicksCheaperOfTwoParallelEdges) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "ExpensiveBridge", 5.0, 500.0, 1000000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{baseUsdc, "CheapBridge", 2.0, 1500.0, 1000000.0, 0.95});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 2.0);
    ASSERT_EQ(route->edges.size(), 1u);
    EXPECT_EQ(route->edges[0].bridgeName, "CheapBridge");
}

TEST(RouteTest, ThrowsOnInvalidSourceIndex) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    EXPECT_THROW(findCheapestRoute(graph, 42, baseUsdc, 1000.0), std::out_of_range);
}

TEST(RouteTest, ThrowsOnInvalidDestinationIndex) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});

    EXPECT_THROW(findCheapestRoute(graph, ethUsdc, 42, 1000.0), std::out_of_range);
}
```

- [ ] **Step 2: Build and run, verify all tests pass**

Run:

```bash
cmake --build router/build
./router/build/tests/chainroute_tests --gtest_filter=RouteTest.*
```

Expected: `PASSED` (10 tests total).

- [ ] **Step 3: Commit**

```bash
git add router/tests/route_test.cpp
git commit -m "test(router): cover parallel edges and invalid NodeIndex arguments"
```

---

### Task 5: Full build verification, correctness review, and reporting

**Files:**
- None created. Potentially modify any file if a warning or failure surfaces.

**Interfaces:**
- Consumes: the complete `chainroute` library (Phase 1 + Phase 2) and `chainroute_tests` binary from Tasks 1-4.
- Produces: a clean, warning-free build; the full Phase 1 + Phase 2 test suite passing; a correctness review of `route.cpp`; the list of files changed; and a line-by-line conceptual explanation of the Dijkstra implementation, all reported to the user.

- [ ] **Step 1: Clean configure and build from scratch, with warnings enabled**

Run:

```bash
rm -rf router/build
cmake -S router -B router/build
cmake --build router/build 2>&1 | tee /tmp/chainroute_phase2_build.log
```

- [ ] **Step 2: Inspect the build log for warnings in project code**

Run:

```bash
grep -i "warning" /tmp/chainroute_phase2_build.log | grep -v "_deps/" || echo "No project-code warnings"
```

If any warning appears in `router/src/` or `router/include/`, fix the underlying code — do not suppress with pragmas. Warnings originating from vendored GoogleTest headers under `_deps/` are not this task's concern. Rebuild until project code is clean.

- [ ] **Step 3: Run the complete Phase 1 + Phase 2 test suite**

Run:

```bash
ctest --test-dir router/build --output-on-failure
```

Expected: all 32 tests pass (22 from Phase 1: `ChainIdTest`, `AssetIdTest`, `NodeTest`, `EdgeTest`, `GraphTest`; 10 from Phase 2: `RouteTest`). If any test fails, use systematic-debugging to find the root cause in `route.cpp` (not the test) unless the test itself is proven wrong, then fix and re-run until green.

- [ ] **Step 4: Review the implementation for correctness against the spec**

Read `router/src/route.cpp` and `router/include/chainroute/route.hpp` fresh, alongside `docs/superpowers/specs/2026-09-10-cpp-dijkstra-routing-design.md`, and confirm:
- The liquidity check (`edge.liquidity < amount`) happens before the distance comparison, so an ineligible edge never influences `dist` even transiently.
- The relaxation comparison is strict (`<`, not `<=`), so parallel-edge tie-breaking is first-encountered-wins as the spec specifies.
- `predNode` and `predEdge` are both written together on every relaxation (never one without the other), so reconstruction can't read a stale/missing pair.
- The stale-entry check (`d > dist[u]`) is `>`, not `>=`, so a freshly-matching entry is still processed.
- `source == destination` genuinely falls out of the general algorithm (no special-cased branch) — confirm by reading the reconstruction loop's condition (`while (current != source)`).
- No `latencyMs` or `reliability` field is read anywhere in `route.cpp`.

If this review finds a real defect, fix it, add a regression test to `router/tests/route_test.cpp` covering the specific scenario, and re-run the full suite before proceeding.

- [ ] **Step 5: List files changed and report to the user**

Run:

```bash
git diff --stat 69355c6..HEAD -- router/
```

(If `69355c6` is not the right pre-Phase-2 commit in this checkout, use `git log --oneline -- router/` to find the last Phase 1 commit and diff from there instead.)

- [ ] **Step 6: Prepare a line-by-line conceptual explanation of route.cpp**

Walk through `router/src/route.cpp` from top to bottom and explain, at a conceptual level (not restating syntax), what each logical block does and why: the index validation, the `dist`/`predNode`/`predEdge` initialization, why the priority queue is seeded with `(0.0, source)`, what the stale-entry check accomplishes and why it's needed with a lazy-deletion heap, why the liquidity check is a `continue` before any distance math, why the relaxation condition is strict `<`, why two predecessor arrays are necessary instead of one, how the reconstruction loop terminates for both the normal case and the `source == destination` case, and why the result is reversed before returning. This becomes the response to the user's request for a line-by-line conceptual explanation — write it out fully; do not defer it.

- [ ] **Step 7: Commit (only if Step 2 or Step 4 required code changes)**

```bash
git add -A
git commit -m "fix(router): resolve issues found during Phase 2 verification"
```

If no changes were needed, skip this commit — there is nothing new to record.
