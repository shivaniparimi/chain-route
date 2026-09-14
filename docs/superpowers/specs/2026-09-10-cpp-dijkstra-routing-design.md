# ChainRoute Phase 2: Cheapest-Fee Routing Design

**Status:** Approved
**Date:** 2026-09-10

## Context

Phase 1 (approved, merged) built the `chainroute` C++ data model: `Node`
(chain+asset pair), `Edge` (bridge/swap with fee, latency, liquidity,
reliability), and `Graph` (an index-based adjacency list,
`std::vector<std::vector<Edge>>`, supporting parallel edges and validating
`NodeIndex` arguments with `std::out_of_range`). No routing algorithm was
implemented in Phase 1 by design.

**Phase 2 scope is the first routing algorithm**: find the cheapest route
(by transaction fee only) from a source node to a destination node, using
Dijkstra's algorithm, subject to a hard liquidity constraint. The Phase 1
`Graph`/`Node`/`Edge` API is not modified in this phase unless a
demonstrated correctness issue requires it.

## Goals

- Given a `Graph`, a source `NodeIndex`, a destination `NodeIndex`, and a
  payment `amount` (USD), find the route that minimizes total fee.
- An edge is only eligible for use if `edge.liquidity >= amount` (hard
  constraint, not a soft cost).
- Correctly handle parallel edges (multiple bridges between the same pair
  of nodes) by considering each independently.
- Return enough information to know which bridges to use, in order.

## Non-goals

- Combining latency or reliability into the routing cost (fee-only for this
  phase).
- Any change to `Graph`, `Node`, or `Edge` beyond what Phase 1 already
  provides.
- Multiple routing strategies/algorithms, a pluggable cost-function
  abstraction, or alternate objectives — Phase 2 is exactly one algorithm
  with exactly one objective (minimize fee).
- Amount validation (a non-positive `amount` is not rejected; it just makes
  every edge trivially liquidity-eligible).

## API & file structure

```
router/include/chainroute/route.hpp   # Route struct + findCheapestRoute declaration
router/src/route.cpp                  # Dijkstra implementation
router/tests/route_test.cpp           # tests (registered in tests/CMakeLists.txt)
```

```cpp
namespace chainroute {

struct Route {
    std::vector<Edge> edges;   // in traversal order, source -> destination
    double totalFee;
};

std::optional<Route> findCheapestRoute(
    const Graph& graph, NodeIndex source, NodeIndex destination, double amount);

}  // namespace chainroute
```

**Rationale for `NodeIndex` parameters:** matches the layering Phase 1
already established — `Graph`'s own API (`addEdge`, `edgesFrom`, `nodeAt`)
is entirely `NodeIndex`-based, and `Node`↔`NodeIndex` resolution is
`Graph::findNode`'s job, not the router's. `findCheapestRoute` stays
decoupled from node lookup/dedup concerns.

**Rationale for `Route` shape:** `edges` alone (no separate node-index
path) avoids carrying two representations of the same path that would need
to stay in sync — the node sequence is recoverable by walking `edge.to`
from the known `source`, and each `Edge` already carries `bridgeName`, the
information a caller actually needs to know which bridge to use at each
hop.

**Validation:** `source`/`destination` are checked against
`graph.nodeCount()` and throw `std::out_of_range` if invalid — the same
convention `Graph` itself already uses for `nodeAt`/`edgesFrom`/`addEdge`.
"No route exists" (disconnected graph, or every candidate edge fails the
liquidity constraint) is not an error — it is a normal outcome, returned as
`std::nullopt`, consistent with how `Graph::findNode` already uses
`std::optional` for "not present."

## How Dijkstra operates against the existing adjacency list

Standard array-based Dijkstra over `Graph`'s adjacency list:

1. `std::vector<double> dist(graph.nodeCount(), infinity)`, `dist[source] = 0`.
2. A min-priority-queue seeded with `(0.0, source)`.
3. Pop the minimum `(d, u)`. If `d > dist[u]`, the entry is stale (a better
   one for `u` was already processed) — discard and continue.
4. Otherwise, call `graph.edgesFrom(u)` and relax each edge in that bucket
   (see below). This is exactly the O(deg(u)) access pattern
   `edgesFrom` was designed to provide.
5. Repeat until the queue is empty.

## What the priority queue contains

```cpp
std::priority_queue<std::pair<double, NodeIndex>,
                     std::vector<std::pair<double, NodeIndex>>,
                     std::greater<>>
```

Pairs of `(tentative distance, node index)`, popped smallest-distance-first.
`std::priority_queue` has no decrease-key operation, so this is a
**lazy-deletion** heap: relaxing a node pushes a new `(dist, node)` pair
rather than updating an existing one in place. Stale entries (superseded by
a later, better push) are simply skipped when popped, per step 3 above.

## Distances and predecessors

- `dist` as above — the public result's `totalFee` is `dist[destination]`.
- Two internal-only parallel arrays, each sized `graph.nodeCount()`:
  - `std::vector<std::optional<NodeIndex>> predNode` — which node the
    best-known path to `v` arrived from.
  - `std::vector<std::optional<Edge>> predEdge` — which specific `Edge`
    (bridge, fee, etc.) was used for that hop.

  Both are required: `Edge` only stores `to`, not `from` (the Phase 1
  design deliberately leaves the source implicit in which adjacency bucket
  an edge lives in), so `predEdge` alone cannot be walked backward — the
  separate `predNode` array is what makes reconstruction possible.

## Reconstructing the result

Starting at `destination`, walk backward via `predNode`/`predEdge` until
reaching `source`, collecting each `predEdge[current]` into a list, then
`std::reverse` that list into source→destination order. Build
`Route{edges, dist[destination]}`.

- If `source == destination`, this loop runs zero times: the result is
  `Route{{}, 0.0}`, which falls directly out of `dist[source] = 0` with no
  special-casing in the algorithm.
- If `dist[destination]` is still infinity once the queue is empty, return
  `std::nullopt` — no path exists under the liquidity constraint.

## Parallel edges

No special-casing anywhere in the algorithm. `edgesFrom(u)` is iterated in
full — every `Edge`, not "the edge to each distinct neighbor" — so two
edges `u → v` with different fees are two independent relaxation
candidates. The cheaper one wins because relaxation only overwrites
`dist[v]`/`predNode[v]`/`predEdge[v]` on strict improvement (`<`, never
`<=`); a later equal-or-worse parallel edge never overwrites a
result already found. This also makes tie-breaking deterministic:
first-encountered-in-`edgesFrom`-order wins.

## Liquidity filtering

A hard skip inside the relaxation loop, before computing any tentative
distance:

```cpp
if (edge.liquidity < amount) continue;
```

A failing edge is never a relaxation candidate, from any node, on every
visit to that node — it does not lower the cost, it is invisible to the
algorithm. A node whose every outgoing edge fails the liquidity check
becomes a dead end. This can legitimately produce `std::nullopt` even on a
graph that would otherwise be connected for a smaller payment amount.

## Time and space complexity

Binary-heap Dijkstra with lazy deletion: **O((V + E) log E) time**,
**O(V + E) auxiliary space**, where `V = graph.nodeCount()` and
`E = graph.edgeCount()`. `Graph` is a multigraph with no bound on parallel
edges between a given pair of nodes, so `E` is not assumed to be O(V²) — the
bound is stated directly in terms of `E`. Each edge causes at most one push
to the queue (a node can be pushed multiple times via different
relaxations), so the queue holds O(E) entries in the worst case, each
push/pop costing O(log E). `dist`, `predNode`, and `predEdge` are each O(V).

## Edge cases and tests

- **Multi-hop cheaper than direct** (the worked example: a 2-hop route at
  $2.50 beats a 1-hop route at $4.00) — the core correctness test.
- **No path exists** (disconnected graph) → `std::nullopt`.
- **`source == destination`** → `Route{{}, 0.0}`.
- **Liquidity filter eliminates the cheapest path's edge** → router falls
  back to a pricier-but-eligible route if one exists, or `std::nullopt` if
  none does.
- **Parallel edges with different fees** → the cheaper edge's `bridgeName`
  appears in the result; the more expensive parallel edge is not used.
- **Invalid `source`/`destination` index** (`>= graph.nodeCount()`) →
  `std::out_of_range`.
- **Isolated destination** (valid index, but no inbound edges reach it) →
  `std::nullopt`.

## Testing approach

`router/tests/route_test.cpp` uses GoogleTest, following the existing
Phase 1 test file conventions (anonymous namespace inside `namespace
chainroute`, constructing a `Graph` via `addNode`/`addEdge` per test). Each
edge case above gets its own test. The worked example from the design
request (Ethereum-USDC → Arbitrum-USDC → Base-USDC at $2.50, beating the
direct $4.00 route) is built explicitly and asserted on both `totalFee` and
the ordered `bridgeName` sequence in `edges`.
