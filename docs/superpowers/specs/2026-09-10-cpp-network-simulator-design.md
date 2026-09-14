# ChainRoute Phase 3: Deterministic Cross-Chain Network Simulator Design

**Status:** Approved
**Date:** 2026-09-10

## Context

Phase 1 (merged) built the `chainroute` C++ data model: `Node` (chain+asset
pair), `Edge` (bridge with fee, latency, liquidity, reliability), and
`Graph` (an index-based adjacency list, supporting parallel edges). Phase 2
(merged) added `findCheapestRoute`: fee-only Dijkstra with liquidity as a
hard eligibility constraint, over `router/include/chainroute/graph.hpp`'s
API — `addNode`, `findNode`, `nodeAt`, `addEdge`, `edgesFrom` (returns
`const std::vector<Edge>&`), `nodeCount`, `edgeCount`. `Graph` has no
mutator beyond append (`addNode`, `addEdge`); there is no `removeEdge`,
`updateEdge`, or `clear`.

**Phase 3 scope is a deterministic simulator** that generates `Graph`
instances reflecting simulated, changing cross-chain network conditions,
without any real blockchain or network connectivity — so the routing
engine can be exercised against realistic, evolving inputs, and so tests
and future benchmarks have a reproducible source of scenarios.

## Goals

- Model the 5 chains (`Ethereum`, `Base`, `Arbitrum`, `Optimism`,
  `Polygon`) × 2 assets (`USDC`, `ETH`) already present in Phase 1's
  `ChainId`/`AssetId` enums, as a fixed 10-node universe.
- Simulate multiple parallel bridges between chain pairs, each with fee,
  latency, liquidity, reliability.
- Produce network conditions that change over simulated time (ticks).
- Be fully deterministic given a seed: same seed ⇒ same topology, same
  metrics at every tick, forever.
- Produce a `Graph` directly usable by `findCheapestRoute`, with zero
  changes to Phase 1/2 code.
- Support updating conditions between ticks.
- Support parallel bridges (reusing `Graph`'s existing multigraph support).
- Produce scenarios where the cheapest route changes over time.
- Produce scenarios where a liquidity change invalidates a previously
  eligible route.
- Some chain pairs may be structurally unreachable (no bridge, either
  direction) — this is desirable, not a defect, since it exercises
  `findCheapestRoute`'s `std::nullopt` path and represents a legitimate
  directed-network condition. Default generation parameters are chosen so
  typical seeded simulations remain useful (not overwhelmingly
  disconnected), not so every pair is guaranteed reachable.

## Non-goals

- Modifying `Graph`, `Node`, or `Edge` in any way, even to make simulation
  more convenient.
- Multi-criteria routing (combining latency/reliability into cost) —
  Phase 2's fee-only Dijkstra is used unmodified against simulator output.
- Same-chain asset swaps (e.g. `Ethereum-USDC → Ethereum-ETH`) — only
  cross-chain bridges for the same asset are modeled.
- A configurable chain/asset universe — hardcoded to the 5×2 set above.
- Any Go, Kafka, PostgreSQL, Redis, Docker, networking, or blockchain API
  code.
- A real-network adapter, or any abstract interface anticipating one (see
  "Future real-network adapter" below).

## API & file structure

```
router/include/chainroute/sim/
    deterministic_rng.hpp   # pure, portable seeded-draw function
    network_simulator.hpp   # NetworkSimulator class
router/src/sim/
    deterministic_rng.cpp
    network_simulator.cpp
router/tests/sim/
    deterministic_rng_test.cpp
    network_simulator_test.cpp
```

New namespace `chainroute::sim`. Files are added to the existing single
`chainroute` library and `chainroute_tests` binary (Phase 1/2's one-binary
convention — no new CMake target).

```cpp
namespace chainroute::sim {

using Seed = std::uint64_t;

class NetworkSimulator {
public:
    explicit NetworkSimulator(Seed seed);

    void tick();                  // advance simulated time by one step
    std::uint64_t currentTick() const;

    Graph snapshot() const;       // build a fresh Graph for the current tick

private:
    Seed seed_;
    std::uint64_t tick_ = 0;
    std::vector<BridgeTemplate> topology_;   // fixed at construction, from seed_
};

}  // namespace chainroute::sim
```

`BridgeTemplate` is an internal (non-public-API) struct holding one
bridge's fixed identity (source `Node`, target `Node`, `bridgeName`) and
base parameters (`baseFee`, `baseLatencyMs`, `baseLiquidity`,
`baseReliability`, `volatility`) — generated once at construction from
`seed_`, never mutated afterward.

## What state the simulator owns

Exactly three things:

- `seed_` — immutable after construction.
- `tick_` — the *only* mutable state; `tick()` is `++tick_`.
- `topology_` — the fixed **set** of bridges (which directed chain pairs
  have bridges, how many, their names and base parameters), generated once
  from the seed and never changed.

The simulator does **not** store current fee/latency/liquidity/reliability
as mutable per-edge state. Those are computed on demand, as a pure
function of `(seed, bridge identity, currentTick)`, inside `snapshot()`.
`tick()` can never desynchronize from what `snapshot()` computes, because
there is nothing stateful to synchronize — and a future `snapshotAt(tick)`
that jumps directly to any tick would be nearly free to add, since
computation doesn't depend on having replayed intermediate ticks.

## Topology generation

Nodes: all 10 `Node{chain, asset}` combinations, added to every `snapshot()`
in a fixed order (see "RNG key construction" below) — seed-independent,
so `NodeIndex` assignment is identical across every simulator instance,
every seed, every tick.

Bridges: **independently determined directed availability** (revised from
an earlier pair-level-bidirectional draft). For each asset, for each of
the 20 ordered chain pairs `(source, target)` with `source != target`, one
independent draw decides inclusion (probability `kPairInclusionProbability
= 0.6`, chosen so a 5-node directed graph with ~12 expected directed edges
per asset is sparse but not degenerate); if included, a second draw
selects a bridge count in `{1, 2, 3}`; each bridge gets its own
independently-drawn name and base parameters. `Ethereum → Base` and
`Base → Ethereum` are entirely independent facts — one, both, or neither
may exist, with unrelated parameters when both do. This was chosen over
guaranteeing bidirectional existence because: (1) real bridge deployments
are frequently asymmetric in practice; (2) `Graph` is intentionally
directed, and guaranteed bidirectionality collapses that distinction at
the topology level, leaving only per-tick noise to ever produce a one-way
route — independent draws instead produce genuine structural one-way
reachability, a distinct and valuable failure/test category; (3) it is
simpler to implement, not more complex — one independent decision per
ordered pair, with no pair-key order-normalization step required.

## How metrics change over tick

Independent per-tick noise around a fixed base (not a stateful random
walk):

```
value(tick) = clamp(base + draw(seed, role, ...) × volatility, validRange)
```

`validRange` per metric: `fee >= 0`, `latencyMs > 0` (never zero or
negative), `0 <= liquidity`, `0 <= reliability <= 1`. Because this is a
pure function of `tick`, `snapshot()` at tick 500 needs no replay of ticks
0–499, and two simulators with the same seed are byte-identical at every
tick by construction.

## Deterministic randomness

`std::uniform_real_distribution` and friends are seeded deterministically,
but their internal algorithm is *unspecified* by the C++ standard — the
same engine and seed can legally produce different draws across standard
library implementations (libstdc++ vs. libc++). `std::hash<std::string>`
has the same problem. Relying on either would mean "reproducible on this
machine" without a guarantee it stays reproducible in CI or on a different
toolchain later — silently violating the explicit reproducibility
requirement.

`deterministic_rng.hpp` therefore implements a small, fully-specified,
self-contained mixing function using only basic integer arithmetic — no
library-provided distribution or hash:

```cpp
double draw(Seed seed, std::string_view role,
            std::uint64_t a = 0, std::uint64_t b = 0,
            std::uint64_t c = 0, std::uint64_t d = 0);
```

`role` combines with `seed` via FNV-1a-style byte hashing; each integer
slot then successively avalanche-mixes into the state (splitmix64-style),
position-sensitively — `draw(seed, "x", 1, 2)` and `draw(seed, "x", 2, 1)`
differ. The result is normalized to `[0, 1)`.

**`role` values are fixed compile-time string literals** (e.g.
`"topo.include"`, `"topo.count"`, `"bridge.name"`, `"bridge.baseFee"`,
`"bridge.baseLatency"`, `"bridge.baseLiquidity"`, `"bridge.baseReliability"`,
`"bridge.volatility"`, `"tick.fee"`, `"tick.latency"`, `"tick.liquidity"`,
`"tick.reliability"`) — never dynamically built or concatenated strings,
eliminating any risk of two logically distinct draws colliding through a
string-formatting bug.

**Integer slots are always stable semantic identifiers**, never anything
derived from container iteration: the `ChainId`/`AssetId` underlying enum
values, a bridge's index within its pair (0, 1, 2 — derived from the
deterministic *count* draw itself, not from container position), and the
tick number. Because `draw`'s output depends only on
`(seed, role, a, b, c, d)`, it is exactly the same regardless of what
order any loop visits pairs in.

**No unordered container is used anywhere in generation.** Topology
generation iterates the 5 chains and 2 assets via `constexpr std::array`s,
never an `unordered_map`/`unordered_set`. This governs not only which
bridges exist (order-independent by the point above) but also the *order
bridges are appended to `topology_`* — which determines edge order within
a `Graph`'s adjacency bucket, and therefore Phase 2's
first-encountered-wins tie-breaking. That order is a fixed, documented
constant: outer loop over `{USDC, ETH}` in that order, then source chain
over the 5 chains in enum-declaration order, then target chain the same
way (skipping `source == target`), then bridges in index order. Node
creation in `snapshot()` follows the same fixed nested order.

## Snapshot architecture

`snapshot() const` builds a local `Graph g`, adds all 10 nodes in the
fixed order above (capturing each returned `NodeIndex` locally — no
`findNode` round-trip needed), then for each `BridgeTemplate` in
`topology_`, computes current-tick metrics via `draw(...)`, clamps them,
and calls `g.addEdge(sourceIndex, Edge{targetIndex, bridgeName, fee,
latencyMs, liquidity, reliability})`. Returns `g` by value.

**Why the returned `Graph` stays effectively immutable:** `Graph` does not
enforce this at the type level — `addNode`/`addEdge` remain public,
non-`const` methods, so nothing stops a caller from mutating a snapshot.
What guarantees it is usage discipline: `NetworkSimulator` never touches a
`Graph` again after returning it, and each `Graph` is a fully independent
value (its members are plain `std::vector`/`std::unordered_map`, so
returning by value is a real, non-aliased copy). This is a documented
contract — treat every snapshot as read-only — not a compiler-enforced
guarantee, since Phase 1's `Graph` API is out of scope to change.

**`NodeIndex` stability:** guaranteed, unconditionally, across every
snapshot from every `NetworkSimulator` instance, any seed, any tick —
because the node universe and its generation order are both fixed and
seed-independent. `Node{Ethereum, USDC}` is always the same index
everywhere.

**Old snapshots after `tick()`:** unaffected. `snapshot()` never returns a
reference into simulator-owned state; `tick()` only changes `tick_`. A
previously-returned `Graph` cannot be reached by any later call.

## Future real-network adapter

The decoupling already exists today, for free: `findCheapestRoute` depends
on nothing but `const Graph&`. It has never heard of `NetworkSimulator`
and never will. A future real adapter's entire job is to produce a `Graph`
— by REST call, on-chain query, database read, or anything else — and
hand it to `findCheapestRoute` exactly as `NetworkSimulator::snapshot()`
does today. No shared base class or virtual interface is required for
that to work.

Per explicit direction, Phase 3 does **not** introduce an abstract
`GraphSource`-style interface. With exactly one implementation and nothing
today that needs to swap sources polymorphically at runtime, that would be
speculative generality — and the real interface questions a live adapter
would raise (sync vs. async, partial-data handling, staleness, timeouts)
can't be meaningfully answered before it's actually built. What Phase 3
does to prepare for that future without building unneeded abstraction:
keeps `NetworkSimulator`'s public surface narrow (`tick`/`currentTick`/
`snapshot`), so a future common interface — if ever justified by 2+ real
implementations needing polymorphic swapping — would be a thin, mechanical
extraction over an already-clean boundary; and keeps `router/src/route.cpp`
/`graph.cpp` structurally unaware that `chainroute::sim` exists (no
`sim/` include anywhere in core Phase 1/2 code).

## Edge cases and tests

Test strategy is golden/fixture-based, not searched at runtime: several
required tests need a specific seed/tick/amount combination that produces
a particular transition (a route flip, a liquidity invalidation). Rather
than searching for one, a small set of literal test seeds is fixed, the
reference implementation is run once during development, and the observed
outputs are hardcoded as expected values — the same pattern Phase 1/2
tests already use for fixed fee/bridgeName literals.

- **Determinism:** same seed + same tick ⇒ byte-identical snapshots (same
  node/edge counts, same edges in the same order, same metric values).
- **Seed sensitivity:** two different fixed seeds ⇒ snapshots differ in at
  least one observable way.
- **Tick evolution:** tick 0 vs. tick 1 on the same simulator ⇒ identical
  topology (same pairs, same bridgeNames, same order) but at least one
  metric differs on at least one edge.
- **Directed asymmetry:** a pinned seed where some chain pair has a bridge
  one direction but not the reverse (or a differing count).
- **Parallel bridges:** a pinned seed/pair with bridge count > 1 ⇒
  `edgesFrom` contains multiple edges to the same target with distinct
  `bridgeName`s.
- **Sparse/multi-hop topology:** a pinned seed with at least one chain
  pair having no direct bridge, combined with `findCheapestRoute`
  returning a multi-hop route or `std::nullopt` for that pair.
- **Route flips over time:** a pinned seed/pair/amount where
  `findCheapestRoute`'s chosen route differs between two specific ticks.
- **Liquidity invalidation:** a pinned seed/pair/amount where a route
  valid at tick T becomes ineligible at tick T′ because an edge's
  liquidity dropped below `amount`, and the router falls back or returns
  `nullopt`.
- **Snapshot isolation:** snapshot at tick 0, advance several ticks,
  snapshot again — the first snapshot's values are unchanged.
- **Metric bounds:** sweep a fixed seed across many ticks (e.g. 0–999) and
  assert every edge's `fee >= 0`, `latencyMs > 0`, `0 <= liquidity`,
  `0 <= reliability <= 1`, on every tick.
- **RNG golden values:** `deterministic_rng_test.cpp` pins exact expected
  `draw(seed, role, a, b, c, d)` outputs for fixed inputs — the
  foundational regression test; if it ever fails, every downstream
  simulation output has silently changed.

## Testing approach

`router/tests/sim/deterministic_rng_test.cpp` and
`network_simulator_test.cpp` use GoogleTest, following the existing
Phase 1/2 conventions (anonymous namespace inside `namespace
chainroute::sim`). Each item in the list above gets its own test.
