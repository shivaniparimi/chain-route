# ChainRoute Phase 1: C++ Routing Graph Design

**Status:** Approved
**Date:** 2026-09-10

## Context

ChainRoute is a cross-chain payment routing system. The eventual project finds
efficient routes for moving digital assets between blockchain networks based
on transaction fees, latency, liquidity, and reliability, and will later
include a Go backend, Kafka, PostgreSQL, Redis, and distributed payment
processing.

**Phase 1 scope is limited to the C++ routing engine's core data model**: the
directed graph that represents blockchain+asset nodes and the bridge/swap
edges between them. Routing algorithms (e.g. Dijkstra) are explicitly out of
scope for this phase — the graph must simply be shaped so that Dijkstra can be
implemented against it later without restructuring.

## Goals

- Model a blockchain+asset pair as a graph node.
- Model a bridge/swap operation between two nodes as a directed edge carrying
  fee, latency, liquidity, and reliability.
- Support multiple parallel edges between the same pair of nodes (e.g. two
  different bridges connecting Ethereum-USDC to Base-USDC).
- Keep the in-memory representation simple and efficient enough to plug a
  shortest-path algorithm into directly, with no restructuring.

## Non-goals

- Implementing any routing/shortest-path algorithm.
- Persistence, networking, or loading graph data from external sources.
- Combining fee/latency/liquidity/reliability into a single routing cost
  function — that is a routing-algorithm concern, not a data-model concern.

## Project structure

The C++ engine lives under the existing `router/` directory, keeping the repo
root free for future services (Go backend, etc.):

```
chainroute/
├── router/
│   ├── CMakeLists.txt
│   ├── include/chainroute/
│   │   ├── chain.hpp      # ChainId enum + toString
│   │   ├── asset.hpp      # AssetId enum + toString
│   │   ├── node.hpp       # Node struct + hash support
│   │   ├── edge.hpp       # Edge struct (fee/latency/liquidity/reliability)
│   │   └── graph.hpp      # Graph class
│   ├── src/
│   │   ├── chain.cpp
│   │   ├── asset.cpp
│   │   ├── node.cpp
│   │   ├── edge.cpp
│   │   └── graph.cpp
│   └── tests/
│       ├── CMakeLists.txt
│       ├── node_test.cpp
│       ├── edge_test.cpp
│       └── graph_test.cpp
├── CLAUDE.md
└── README.md
```

- **Build system:** CMake, targeting C++20.
- **Testing:** GoogleTest, pulled in via CMake `FetchContent` (no manual
  dependency install). A `tests/` target is set up now so future algorithm
  work has a test pattern established from day one.
- **Namespace:** everything lives in `chainroute`.

## Data model

### Node

```cpp
enum class ChainId : uint8_t { Ethereum, Base, Arbitrum, Optimism, Polygon };
enum class AssetId : uint8_t { USDC, USDT, ETH, WBTC, DAI };

struct Node {
    ChainId chain;
    AssetId asset;

    friend bool operator==(const Node&, const Node&) = default;
};

struct NodeHash {
    size_t operator()(const Node& n) const noexcept;
};

std::string toString(const Node& n); // e.g. "Ethereum-USDC", for logging only
```

**Rationale:** A `Node` is two small enums — a cheap, trivially-copyable POD.
Enums give compile-time type safety (an asset can't accidentally be passed
where a chain is expected) and catch typos that string IDs would not. The
defaulted `operator==` (C++20) plus a custom hash let `Node` be used directly
as an `unordered_map` key for deduplication. String identifiers are generated
only for display/logging, never used as the source of truth for identity.

### Edge

```cpp
using NodeIndex = std::size_t;

struct Edge {
    NodeIndex to;             // target node index; source is implicit (see Graph)
    std::string bridgeName;   // e.g. "Stargate", "Uniswap-V3" — disambiguates parallel edges
    double fee;                // absolute cost, USD
    double latencyMs;          // expected execution time, milliseconds
    double liquidity;          // available liquidity, USD
    double reliability;        // success probability, [0.0, 1.0]
};
```

**Rationale:** An `Edge` stores only its target index, not both endpoints,
because edges live inside the source node's adjacency bucket in `Graph` — the
source is always implicit from context. `bridgeName` is what makes the
multigraph meaningful: it lets a future routing algorithm distinguish, e.g.,
"Stargate: cheap but slow" from "Wormhole: fast but less reliable" between the
same two nodes. All four metrics are raw `double`s with documented units;
Phase 1 only models the data, it does not decide how to combine them into a
routing cost.

### Graph

```cpp
class Graph {
public:
    NodeIndex addNode(const Node& node);               // idempotent: returns existing index if present
    std::optional<NodeIndex> findNode(const Node& node) const;
    const Node& nodeAt(NodeIndex idx) const;

    void addEdge(NodeIndex from, Edge edge);            // push_back: supports parallel edges
    const std::vector<Edge>& edgesFrom(NodeIndex idx) const;

    std::size_t nodeCount() const;
    std::size_t edgeCount() const;

private:
    std::vector<Node> nodes_;                                   // index -> Node
    std::unordered_map<Node, NodeIndex, NodeHash> nodeIndex_;   // Node -> index (dedup)
    std::vector<std::vector<Edge>> adjacency_;                  // adjacency_[i] = edges out of node i
};
```

## In-memory storage rationale

This is an **index-based adjacency list**, not an adjacency matrix:

- `nodes_` is append-only, giving every node a stable integer index for O(1)
  lookup — needed later to reconstruct a path or print node identities.
- `nodeIndex_` gives O(1) `Node → index` lookup so `addNode` can dedupe
  (calling it twice with the same Chain/Asset returns the same index instead
  of creating a duplicate).
- `adjacency_[i]` holds only the real outgoing edges of node `i`, including
  multiple entries to the same target (different bridges). This is
  sparse-friendly: the graph will have relatively few bridges per node
  compared to the total number of Chain×Asset pairs, so an `N×N` matrix
  (mostly empty, and awkward for multi-edges) would waste memory and still
  need per-cell vectors to support parallel edges anyway.

## Why this is Dijkstra-ready

- `NodeIndex` is a plain integer, so a future Dijkstra implementation can use
  flat `std::vector<double> dist(graph.nodeCount())` and
  `std::vector<NodeIndex> prev(...)` instead of hash-map-based distance
  tracking.
- `edgesFrom(idx)` gives exactly the O(deg(idx)) edges needed for the
  relaxation step — no filtering, no matrix row scan.
- Because `Edge` already carries fee/latency/liquidity/reliability as
  separate fields, a routing algorithm can later plug in whatever cost
  function it wants (e.g. `cost = fee + latencyMs * k`) without changing the
  graph structure at all.

## Testing approach

`tests/` uses GoogleTest for construction/behavior tests of `Node`, `Edge`,
and `Graph`: node equality/hashing, `addNode` idempotency, `addEdge` allowing
parallel edges between the same pair, and `edgesFrom` returning the correct
set. No algorithm behavior is tested in this phase since none is implemented.
