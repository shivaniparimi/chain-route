# ChainRoute Phase 4a: Proto Contract + C++ Routing Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the shared `routing.proto` contract and the C++ gRPC
`RoutingService` (`cpp-routing-service/`) per
`docs/superpowers/specs/2026-09-10-grpc-service-design.md`, standalone and
fully testable without the Go service existing yet. Plan B (Go API + E2E)
depends on this plan's generated proto and follows separately.

**Architecture:** A new CMake project (`cpp-routing-service/`) alongside
`router/`, depending on it via `add_subdirectory`. A thin
`RoutingServiceImpl` resolves `(Chain, Asset)` proto values to internal
`ChainId`/`AssetId` via an explicit conversion layer, calls the unmodified
Phase 1-3 `NetworkSimulator`/`findCheapestRoute`, and converts the result
back — `NodeIndex` never leaves the request-handling function.

**Tech Stack:** C++20, CMake, system-installed protobuf + gRPC (via
Homebrew), GoogleTest (existing FetchContent pattern, unchanged).

## Global Constraints

- Do NOT modify any file under `router/` (Phase 1-3 source, headers, or
  tests). This plan only adds new files under `proto/` and
  `cpp-routing-service/`.
- `router/` stays dependency-free of gRPC/protobuf — `cpp-routing-service`
  is a separate CMake project that depends on `router/`, never the other
  way around.
- `NodeIndex` must never appear in a protobuf message or be logged/exposed
  in any client-visible way. It exists only as a local variable inside
  `RoutingServiceImpl::FindRoute`.
- The proto contract is exactly as specified: one package
  (`chainroute.v1`), two enums (`Chain`, `Asset`, each with an
  `_UNSPECIFIED = 0` value), `FindRouteRequest`, `RouteHop`,
  `FindRouteResponse`, one RPC `RoutingService.FindRoute`.
- `toChainId`/`toAssetId` return `std::optional` (invalid/unspecified wire
  values are expected input, not thrown errors); `toProtoChain`/
  `toProtoAsset` are implemented as exhaustive `switch` statements over
  the full `ChainId`/`AssetId` enums (compiler-enforced via `-Wswitch`).
- `RoutingServiceImpl::FindRoute` validates defensively (invalid chain/
  asset/amount) and returns `grpc::StatusCode::INVALID_ARGUMENT` — this is
  belt-and-suspenders behind Go's validation (Plan B), not a replacement
  for it.
- "No route found" is `grpc::Status::OK` with `route_found = false`, never
  an error status.
- The C++ service owns exactly one `NetworkSimulator`, constructed once at
  startup with a `--seed` flag (default provided), and never calls
  `tick()` — the simulator stays fixed for the process's lifetime.
- The server registers the standard `grpc.health.v1.Health` service via
  gRPC's built-in default health check service — no custom health RPC.
- Graceful shutdown: `SIGINT`/`SIGTERM` calls `grpc::Server::Shutdown()`.
- No Kafka/PostgreSQL/Redis/Docker/Kubernetes/blockchain API code. No
  retries. No multi-criteria routing (only `fee` is read for cost; all
  four metrics are copied into the response as data).
- Build with `-Wall -Wextra -Wpedantic` on non-MSVC and keep project code
  warning-free, matching `router/`'s existing convention.

## Environment note

This environment currently has `protoc` (via anaconda) but no
`grpc++`/`protobuf` C++ development libraries, and no `grpc_cpp_plugin`.
Task 1 installs what's needed via Homebrew. The exact CMake incantation to
locate a Homebrew-installed gRPC/protobuf via `find_package` can vary
slightly by version — Task 1's steps include a concrete, standard attempt
plus explicit troubleshooting guidance, since this is the one place in
this plan where environment-specific iteration is expected, similar in
spirit to how Phase 3's RNG golden values had to be discovered by
execution rather than specified in advance.

---

### Task 1: Environment setup, proto contract, and CMake scaffolding

**Files:**
- Create: `proto/chainroute/v1/routing.proto`
- Create: `cpp-routing-service/CMakeLists.txt`
- Create: `cpp-routing-service/src/main.cpp` (placeholder — replaced fully in Task 4)
- Create: `cpp-routing-service/tests/CMakeLists.txt`

**Interfaces:**
- Produces: a configuring, building CMake project that generates C++
  stubs from `routing.proto` and links a trivial executable against them,
  proving the toolchain works before any real logic is written.

- [ ] **Step 1: Install the missing toolchain**

Run:

```bash
brew install grpc protobuf go
brew --prefix grpc
brew --prefix protobuf
```

Confirm `grpc_cpp_plugin` is now on `PATH` (Homebrew's `grpc` formula
installs it):

```bash
which grpc_cpp_plugin
```

If it's not found, it's typically under `$(brew --prefix grpc)/bin` —
note that path; Task 1 Step 4's CMake will need `find_program` to locate
it, which searches `CMAKE_PREFIX_PATH`.

(`go` is installed now so it's available for Plan B later; this plan
doesn't use it.)

- [ ] **Step 2: Write the proto contract**

`proto/chainroute/v1/routing.proto`:

```protobuf
syntax = "proto3";

package chainroute.v1;

option go_package = "chainroute/go-api/internal/gen/chainroute/v1;routingv1";

enum Chain {
  CHAIN_UNSPECIFIED = 0;
  CHAIN_ETHEREUM = 1;
  CHAIN_BASE = 2;
  CHAIN_ARBITRUM = 3;
  CHAIN_OPTIMISM = 4;
  CHAIN_POLYGON = 5;
}

enum Asset {
  ASSET_UNSPECIFIED = 0;
  ASSET_USDC = 1;
  ASSET_ETH = 2;
}

message FindRouteRequest {
  Chain source_chain = 1;
  Chain destination_chain = 2;
  Asset asset = 3;
  double amount = 4;
}

message RouteHop {
  Chain from_chain = 1;
  Chain to_chain = 2;
  string bridge_name = 3;
  double fee = 4;
  double latency_ms = 5;
  double liquidity = 6;
  double reliability = 7;
}

message FindRouteResponse {
  bool route_found = 1;
  repeated RouteHop hops = 2;
  double total_fee = 3;
}

service RoutingService {
  rpc FindRoute(FindRouteRequest) returns (FindRouteResponse);
}
```

- [ ] **Step 3: Write a placeholder main.cpp**

`cpp-routing-service/src/main.cpp`:

```cpp
int main() {
    return 0;
}
```

(Replaced with the real server in Task 4. This exists now only so the
CMake project has something to build and link in Step 4's verification.)

- [ ] **Step 4: Write the CMake scaffolding**

`cpp-routing-service/CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(chainroute_service CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../router ${CMAKE_CURRENT_BINARY_DIR}/router-build)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
set(PROTO_FILE ${PROTO_DIR}/chainroute/v1/routing.proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRCS ${PROTO_GEN_DIR}/chainroute/v1/routing.pb.cc)
set(PROTO_HDRS ${PROTO_GEN_DIR}/chainroute/v1/routing.pb.h)
set(GRPC_SRCS ${PROTO_GEN_DIR}/chainroute/v1/routing.grpc.pb.cc)
set(GRPC_HDRS ${PROTO_GEN_DIR}/chainroute/v1/routing.grpc.pb.h)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protobuf::protoc
    ARGS --cpp_out=${PROTO_GEN_DIR}
         --grpc_out=${PROTO_GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         -I ${PROTO_DIR}
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating C++ protobuf/gRPC code from routing.proto"
)

add_library(chainroute_proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(chainroute_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(chainroute_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(chainroute_service_server src/main.cpp)
target_link_libraries(chainroute_service_server PRIVATE chainroute_proto)

if (NOT MSVC)
    target_compile_options(chainroute_proto PRIVATE -w)  # generated code, not ours
endif()

enable_testing()
add_subdirectory(tests)
```

`cpp-routing-service/tests/CMakeLists.txt` (empty placeholder for now,
filled in Task 2):

```cmake
# Test targets are added in Task 2 onward.
```

- [ ] **Step 5: Configure and build, verify the toolchain works end to end**

Run:

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build
```

Expected: configures and builds successfully, producing
`cpp-routing-service/build/chainroute_service_server`. This proves protoc
generation, the gRPC C++ plugin, and linking against `protobuf::libprotobuf`
and `gRPC::grpc++` all work.

**If `find_package(Protobuf CONFIG REQUIRED)` or `find_package(gRPC CONFIG
REQUIRED)` fails:** Homebrew's CMake config files usually aren't on
CMake's default search path on macOS. Retry with:

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build \
    -DCMAKE_PREFIX_PATH="$(brew --prefix)"
```

If that still fails, check `$(brew --prefix grpc)/lib/cmake` and
`$(brew --prefix protobuf)/lib/cmake` exist and add both explicitly via
`-DCMAKE_PREFIX_PATH="$(brew --prefix grpc);$(brew --prefix protobuf)"`.
This is the one step in this plan most likely to need environment-specific
adjustment — iterate here until it configures cleanly before moving on;
do not proceed to Task 2 with a broken build.

- [ ] **Step 6: Commit**

```bash
git add proto/ cpp-routing-service/
git commit -m "feat(service): add routing.proto contract and C++ service CMake scaffolding"
```

---

### Task 2: Chain/Asset conversion layer

**Files:**
- Create: `cpp-routing-service/include/chainroute_service/chain_asset_convert.hpp`
- Create: `cpp-routing-service/src/chain_asset_convert.cpp`
- Create: `cpp-routing-service/tests/chain_asset_convert_test.cpp`
- Modify: `cpp-routing-service/CMakeLists.txt` (add a `chainroute_service_lib` library target, GoogleTest FetchContent, and register the test)
- Modify: `cpp-routing-service/tests/CMakeLists.txt` (add the test executable)

**Interfaces:**
- Consumes: `chainroute::ChainId`, `chainroute::AssetId` (Phase 1,
  `router/include/chainroute/chain.hpp`/`asset.hpp`, unchanged);
  `chainroute::v1::Chain`, `chainroute::v1::Asset` (Task 1's generated
  proto).
- Produces:
```cpp
namespace chainroute_service {
std::optional<chainroute::ChainId> toChainId(chainroute::v1::Chain proto);
std::optional<chainroute::AssetId> toAssetId(chainroute::v1::Asset proto);
chainroute::v1::Chain toProtoChain(chainroute::ChainId chain);
chainroute::v1::Asset toProtoAsset(chainroute::AssetId asset);
}
```

- [ ] **Step 1: Add a chainroute_service_lib library target and GoogleTest to CMake**

In `cpp-routing-service/CMakeLists.txt`, add, after the `chainroute_proto`
target and before `add_executable(chainroute_service_server ...)`:

```cmake
add_library(chainroute_service_lib STATIC
    src/chain_asset_convert.cpp
)
target_include_directories(chainroute_service_lib PUBLIC include)
target_link_libraries(chainroute_service_lib PUBLIC chainroute chainroute_proto)
if (NOT MSVC)
    target_compile_options(chainroute_service_lib PRIVATE -Wall -Wextra -Wpedantic)
endif()
```

Change `add_executable(chainroute_service_server src/main.cpp)`'s
`target_link_libraries` line to also link `chainroute_service_lib`:

```cmake
target_link_libraries(chainroute_service_server PRIVATE chainroute_service_lib chainroute_proto)
```

Replace `cpp-routing-service/tests/CMakeLists.txt` entirely with:

```cmake
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(chainroute_service_tests
    chain_asset_convert_test.cpp
)
target_link_libraries(chainroute_service_tests PRIVATE chainroute_service_lib GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(chainroute_service_tests)
```

- [ ] **Step 2: Write the failing tests**

`cpp-routing-service/tests/chain_asset_convert_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {
namespace {

TEST(ChainAssetConvertTest, RoundTripsEveryChain) {
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Ethereum)), chainroute::ChainId::Ethereum);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Base)), chainroute::ChainId::Base);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Arbitrum)), chainroute::ChainId::Arbitrum);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Optimism)), chainroute::ChainId::Optimism);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Polygon)), chainroute::ChainId::Polygon);
}

TEST(ChainAssetConvertTest, RoundTripsEveryUsedAsset) {
    EXPECT_EQ(toAssetId(toProtoAsset(chainroute::AssetId::USDC)), chainroute::AssetId::USDC);
    EXPECT_EQ(toAssetId(toProtoAsset(chainroute::AssetId::ETH)), chainroute::AssetId::ETH);
}

TEST(ChainAssetConvertTest, UnspecifiedChainMapsToNullopt) {
    EXPECT_FALSE(toChainId(chainroute::v1::CHAIN_UNSPECIFIED).has_value());
}

TEST(ChainAssetConvertTest, UnspecifiedAssetMapsToNullopt) {
    EXPECT_FALSE(toAssetId(chainroute::v1::ASSET_UNSPECIFIED).has_value());
}

TEST(ChainAssetConvertTest, OutOfRangeWireValueMapsToNullopt) {
    EXPECT_FALSE(toChainId(static_cast<chainroute::v1::Chain>(999)).has_value());
    EXPECT_FALSE(toAssetId(static_cast<chainroute::v1::Asset>(999)).has_value());
}

TEST(ChainAssetConvertTest, ToProtoChainProducesExpectedValues) {
    EXPECT_EQ(toProtoChain(chainroute::ChainId::Ethereum), chainroute::v1::CHAIN_ETHEREUM);
    EXPECT_EQ(toProtoChain(chainroute::ChainId::Polygon), chainroute::v1::CHAIN_POLYGON);
}

TEST(ChainAssetConvertTest, ToProtoAssetProducesExpectedValues) {
    EXPECT_EQ(toProtoAsset(chainroute::AssetId::USDC), chainroute::v1::ASSET_USDC);
    EXPECT_EQ(toProtoAsset(chainroute::AssetId::ETH), chainroute::v1::ASSET_ETH);
}

}  // namespace
}  // namespace chainroute_service
```

- [ ] **Step 3: Write the header**

`cpp-routing-service/include/chainroute_service/chain_asset_convert.hpp`:

```cpp
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
```

- [ ] **Step 4: Build and verify the tests fail to link (header exists, no implementation yet)**

Run:

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build
```

Expected: undefined-reference linker errors for `toChainId`, `toAssetId`,
`toProtoChain`, `toProtoAsset`.

- [ ] **Step 5: Implement the conversion functions**

`cpp-routing-service/src/chain_asset_convert.cpp`:

```cpp
#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {

std::optional<chainroute::ChainId> toChainId(chainroute::v1::Chain proto) {
    switch (proto) {
        case chainroute::v1::CHAIN_ETHEREUM: return chainroute::ChainId::Ethereum;
        case chainroute::v1::CHAIN_BASE: return chainroute::ChainId::Base;
        case chainroute::v1::CHAIN_ARBITRUM: return chainroute::ChainId::Arbitrum;
        case chainroute::v1::CHAIN_OPTIMISM: return chainroute::ChainId::Optimism;
        case chainroute::v1::CHAIN_POLYGON: return chainroute::ChainId::Polygon;
        case chainroute::v1::CHAIN_UNSPECIFIED:
        default:
            return std::nullopt;
    }
}

std::optional<chainroute::AssetId> toAssetId(chainroute::v1::Asset proto) {
    switch (proto) {
        case chainroute::v1::ASSET_USDC: return chainroute::AssetId::USDC;
        case chainroute::v1::ASSET_ETH: return chainroute::AssetId::ETH;
        case chainroute::v1::ASSET_UNSPECIFIED:
        default:
            return std::nullopt;
    }
}

chainroute::v1::Chain toProtoChain(chainroute::ChainId chain) {
    switch (chain) {
        case chainroute::ChainId::Ethereum: return chainroute::v1::CHAIN_ETHEREUM;
        case chainroute::ChainId::Base: return chainroute::v1::CHAIN_BASE;
        case chainroute::ChainId::Arbitrum: return chainroute::v1::CHAIN_ARBITRUM;
        case chainroute::ChainId::Optimism: return chainroute::v1::CHAIN_OPTIMISM;
        case chainroute::ChainId::Polygon: return chainroute::v1::CHAIN_POLYGON;
    }
    return chainroute::v1::CHAIN_UNSPECIFIED;
}

chainroute::v1::Asset toProtoAsset(chainroute::AssetId asset) {
    switch (asset) {
        case chainroute::AssetId::USDC: return chainroute::v1::ASSET_USDC;
        case chainroute::AssetId::ETH: return chainroute::v1::ASSET_ETH;
        case chainroute::AssetId::USDT:
        case chainroute::AssetId::WBTC:
        case chainroute::AssetId::DAI:
            return chainroute::v1::ASSET_UNSPECIFIED;
    }
    return chainroute::v1::ASSET_UNSPECIFIED;
}

}  // namespace chainroute_service
```

Note: `USDT`/`WBTC`/`DAI` are unreachable in practice (the simulator's
fixed universe only ever produces `USDC`/`ETH`), but are listed explicitly
so the switch stays exhaustive and compiler-checked against the full
`AssetId` enum — if a 6th asset is ever added to `AssetId`, this switch
fails to compile until updated.

- [ ] **Step 6: Build and run, verify all tests pass**

Run:

```bash
cmake --build cpp-routing-service/build
./cpp-routing-service/build/tests/chainroute_service_tests --gtest_filter=ChainAssetConvertTest.*
```

Expected: `PASSED` (7 tests).

- [ ] **Step 7: Commit**

```bash
git add cpp-routing-service/
git commit -m "feat(service): add Chain/Asset <-> ChainId/AssetId conversion layer"
```

---

### Task 3: RoutingServiceImpl (business logic)

**Files:**
- Create: `cpp-routing-service/include/chainroute_service/routing_service.hpp`
- Create: `cpp-routing-service/src/routing_service.cpp`
- Create: `cpp-routing-service/tests/routing_service_test.cpp`
- Modify: `cpp-routing-service/CMakeLists.txt` (add `routing_service.cpp` to `chainroute_service_lib`)
- Modify: `cpp-routing-service/tests/CMakeLists.txt` (add the new test file)

**Interfaces:**
- Consumes: `chainroute::sim::NetworkSimulator` (Phase 3, unchanged),
  `chainroute::findCheapestRoute`/`Route` (Phase 2, unchanged),
  `chainroute::Graph`/`Node`/`Edge`/`NodeIndex` (Phase 1, unchanged),
  `toChainId`/`toAssetId`/`toProtoChain`/`toProtoAsset` (Task 2).
- Produces:
```cpp
namespace chainroute_service {
class RoutingServiceImpl final : public chainroute::v1::RoutingService::Service {
public:
    explicit RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator);
    grpc::Status FindRoute(grpc::ServerContext* context,
                            const chainroute::v1::FindRouteRequest* request,
                            chainroute::v1::FindRouteResponse* response) override;
private:
    chainroute::sim::NetworkSimulator& simulator_;
};
}
```

- [ ] **Step 1: Add routing_service.cpp to CMake**

In `cpp-routing-service/CMakeLists.txt`, add `src/routing_service.cpp` to
`chainroute_service_lib`'s source list:

```cmake
add_library(chainroute_service_lib STATIC
    src/chain_asset_convert.cpp
    src/routing_service.cpp
)
```

In `cpp-routing-service/tests/CMakeLists.txt`, add
`routing_service_test.cpp` to `chainroute_service_tests`'s source list:

```cmake
add_executable(chainroute_service_tests
    chain_asset_convert_test.cpp
    routing_service_test.cpp
)
```

- [ ] **Step 2: Write the failing tests**

`cpp-routing-service/tests/routing_service_test.cpp`:

```cpp
#include <gtest/gtest.h>

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute_service/routing_service.hpp"

namespace chainroute_service {
namespace {

TEST(RoutingServiceTest, ReturnsARouteForAValidRequest) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    if (response.route_found()) {
        EXPECT_GT(response.hops_size(), 0);
        EXPECT_EQ(response.hops(0).from_chain(), chainroute::v1::CHAIN_ETHEREUM);
        EXPECT_EQ(response.hops(response.hops_size() - 1).to_chain(), chainroute::v1::CHAIN_BASE);
        EXPECT_FALSE(response.hops(0).bridge_name().empty());
        EXPECT_GE(response.total_fee(), 0.0);
    }
}

TEST(RoutingServiceTest, ReturnsRouteFoundFalseWhenNoNodesConnectDirectlyOrAtAll) {
    // Use an amount so large that liquidity can never cover it (Phase 3's
    // liquidity ceiling is a few million) -- guarantees no eligible route,
    // exercising the normal "no route" outcome deterministically for any seed.
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1e12);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
    EXPECT_DOUBLE_EQ(response.total_fee(), 0.0);
}

TEST(RoutingServiceTest, RejectsUnspecifiedSourceChain) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_UNSPECIFIED);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST(RoutingServiceTest, RejectsUnspecifiedAsset) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_UNSPECIFIED);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST(RoutingServiceTest, RejectsNonPositiveAmount) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(0.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

}  // namespace
}  // namespace chainroute_service
```

- [ ] **Step 3: Write the header**

`cpp-routing-service/include/chainroute_service/routing_service.hpp`:

```cpp
#pragma once

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute/v1/routing.grpc.pb.h"

namespace chainroute_service {

class RoutingServiceImpl final : public chainroute::v1::RoutingService::Service {
public:
    explicit RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator);

    grpc::Status FindRoute(grpc::ServerContext* context,
                            const chainroute::v1::FindRouteRequest* request,
                            chainroute::v1::FindRouteResponse* response) override;

private:
    chainroute::sim::NetworkSimulator& simulator_;
};

}  // namespace chainroute_service
```

- [ ] **Step 4: Build and verify the tests fail to link**

Run:

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build
```

Expected: undefined-reference or vtable-related linker errors for
`RoutingServiceImpl`.

- [ ] **Step 5: Implement RoutingServiceImpl**

`cpp-routing-service/src/routing_service.cpp`:

```cpp
#include "chainroute_service/routing_service.hpp"

#include "chainroute/route.hpp"
#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {

RoutingServiceImpl::RoutingServiceImpl(chainroute::sim::NetworkSimulator& simulator)
    : simulator_(simulator) {}

grpc::Status RoutingServiceImpl::FindRoute(
    grpc::ServerContext* /*context*/,
    const chainroute::v1::FindRouteRequest* request,
    chainroute::v1::FindRouteResponse* response) {
    const auto sourceChain = toChainId(request->source_chain());
    const auto destChain = toChainId(request->destination_chain());
    const auto asset = toAssetId(request->asset());

    if (!sourceChain.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid source_chain");
    }
    if (!destChain.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid destination_chain");
    }
    if (!asset.has_value()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid asset");
    }
    if (request->amount() <= 0.0) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "amount must be positive");
    }

    const chainroute::Graph graph = simulator_.snapshot();

    const auto sourceNode = graph.findNode(chainroute::Node{*sourceChain, *asset});
    const auto destNode = graph.findNode(chainroute::Node{*destChain, *asset});
    if (!sourceNode.has_value() || !destNode.has_value()) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "node not present in snapshot");
    }

    const auto route = chainroute::findCheapestRoute(graph, *sourceNode, *destNode, request->amount());

    if (!route.has_value()) {
        response->set_route_found(false);
        response->set_total_fee(0.0);
        return grpc::Status::OK;
    }

    response->set_route_found(true);
    response->set_total_fee(route->totalFee);

    chainroute::NodeIndex current = *sourceNode;
    for (const chainroute::Edge& edge : route->edges) {
        chainroute::v1::RouteHop* hop = response->add_hops();
        hop->set_from_chain(toProtoChain(graph.nodeAt(current).chain));
        hop->set_to_chain(toProtoChain(graph.nodeAt(edge.to).chain));
        hop->set_bridge_name(edge.bridgeName);
        hop->set_fee(edge.fee);
        hop->set_latency_ms(edge.latencyMs);
        hop->set_liquidity(edge.liquidity);
        hop->set_reliability(edge.reliability);
        current = edge.to;
    }

    return grpc::Status::OK;
}

}  // namespace chainroute_service
```

Note the `NodeIndex` usage here: `sourceNode`, `destNode`, and `current`
are all `chainroute::NodeIndex` (really `std::size_t`), used only to walk
`graph`/`route` and immediately converted to proto `Chain` values before
being written into `response`. No `NodeIndex` value is ever stored in
`response` or returned from this function.

- [ ] **Step 6: Build and run, verify all tests pass**

Run:

```bash
cmake --build cpp-routing-service/build
./cpp-routing-service/build/tests/chainroute_service_tests
```

Expected: `PASSED` (12 tests: 7 from Task 2 + 5 new).

- [ ] **Step 7: Commit**

```bash
git add cpp-routing-service/
git commit -m "feat(service): add RoutingServiceImpl wiring proto requests to Phase 1-3 routing"
```

---

### Task 4: Real server (main.cpp) with health service and graceful shutdown

**Files:**
- Modify: `cpp-routing-service/src/main.cpp` (replace the placeholder)

**Interfaces:**
- Consumes: `RoutingServiceImpl` (Task 3), `NetworkSimulator` (Phase 3).
- Produces: a runnable gRPC server binary listening on a configurable
  address, serving `RoutingService` and the standard
  `grpc.health.v1.Health` service, shutting down gracefully on
  `SIGINT`/`SIGTERM`.

- [ ] **Step 1: Replace main.cpp**

`cpp-routing-service/src/main.cpp`:

```cpp
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute_service/routing_service.hpp"

namespace {

std::unique_ptr<grpc::Server> g_server;

void handleSignal(int /*signal*/) {
    if (g_server) {
        g_server->Shutdown();
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::uint64_t seed = 42;
    std::string listenAddress = "0.0.0.0:50051";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--seed=", 0) == 0) {
            seed = std::strtoull(arg.c_str() + 7, nullptr, 10);
        } else if (arg.rfind("--listen-address=", 0) == 0) {
            listenAddress = arg.substr(std::strlen("--listen-address="));
        }
    }

    chainroute::sim::NetworkSimulator simulator(seed);
    chainroute_service::RoutingServiceImpl service(simulator);

    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listenAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    g_server = builder.BuildAndStart();
    if (!g_server) {
        std::cerr << "Failed to start server on " << listenAddress << "\n";
        return 1;
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    std::cout << "chainroute_service_server listening on " << listenAddress
              << " (seed=" << seed << ")\n";
    g_server->Wait();
    std::cout << "chainroute_service_server shut down\n";
    return 0;
}
```

- [ ] **Step 2: Build**

Run:

```bash
cmake --build cpp-routing-service/build
```

Expected: builds successfully.

- [ ] **Step 3: Manually verify the server starts, serves health checks, and shuts down gracefully**

Run the server in the background, on a scratch port:

```bash
./cpp-routing-service/build/chainroute_service_server --seed=1001 --listen-address=127.0.0.1:50099 &
SERVER_PID=$!
sleep 1
```

Verify the health service responds. If `grpc_health_probe` is available
(`brew install grpc-health-probe` or check if it's already on `PATH`),
use it:

```bash
grpc_health_probe -addr=127.0.0.1:50099
```

Expected output indicates `SERVING`. If `grpc_health_probe` isn't
available, this can instead be verified in Task 5's test client (which
will directly construct a `grpc::health::v1::Health::Stub` and call
`Check`), or by installing it now (`brew install grpc-health-probe`) since
Plan B's E2E script will also want it (or an equivalent) for readiness
polling — install it now if it's not already present, since it's the
simplest, most standard tool for this and matches "prefer standard
grpc.health.v1 ... do not introduce disproportionate complexity."

**If the health check does not report SERVING immediately:** gRPC C++'s
default health check service, once enabled via
`grpc::EnableDefaultHealthCheckService(true)`, is documented to report
`SERVING` for the empty service name (the "overall" server health) once
the server starts — no manual `SetServingStatus` call should be needed
for this plan's purposes (checking the overall server, not a specific
service by name). If it reports `NOT_SERVING` or is unreachable, check
that `builder.RegisterService(&service)` ran before `BuildAndStart()`
(it does, per the code above) and that the health check service was
enabled before constructing `ServerBuilder` (it is). This is the other
spot in this plan (alongside Task 1's CMake `find_package` step) where
minor version-specific iteration may be needed — consult gRPC's official
C++ health checking example if the above doesn't work as described.

Then verify graceful shutdown:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

Expected: the process prints `chainroute_service_server shut down` and
exits with code 0 (or the shell reports it exited cleanly), not killed
abruptly.

- [ ] **Step 4: Commit**

```bash
git add cpp-routing-service/src/main.cpp
git commit -m "feat(service): add real gRPC server main with health service and graceful shutdown"
```

---

### Task 5: Plan A verification

**Files:**
- None created. Potentially modify any file if a warning or failure
  surfaces.

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: a clean, warning-free build; `router/`'s 49 tests plus this
  plan's new C++ service tests all passing; confirmation `router/` is
  byte-identical to before this plan; a runnable server verified
  end-to-end at the gRPC layer (no Go involved yet — that's Plan B).

- [ ] **Step 1: Clean build of router/ alone, confirm untouched and still passing**

Run:

```bash
rm -rf router/build
cmake -S router -B router/build
cmake --build router/build
ctest --test-dir router/build --output-on-failure
```

Expected: all 49 Phase 1-3 tests pass, unchanged from before this plan.

- [ ] **Step 2: Clean build of cpp-routing-service, with warnings enabled**

Run:

```bash
rm -rf cpp-routing-service/build
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build 2>&1 | tee /tmp/chainroute_service_build.log
grep -E "cpp-routing-service/(src|include)/" /tmp/chainroute_service_build.log || echo "No project-code warnings"
```

If any warning appears in `cpp-routing-service/src/` or
`cpp-routing-service/include/` (not the generated proto code under
`build/generated/`, which is excluded from warnings via the `-w` flag set
in Task 1), fix the underlying code and rebuild until clean.

- [ ] **Step 3: Run the C++ service test suite**

Run:

```bash
ctest --test-dir cpp-routing-service/build --output-on-failure
```

Expected: all 12 `chainroute_service_tests` pass.

- [ ] **Step 4: Verify router/ was not modified by this plan**

Run:

```bash
git diff --stat 4a898b5..HEAD -- router/
```

(If `4a898b5` is not the right pre-Plan-A commit in this checkout, use
`git log --oneline -- router/` to find the last commit that touched
`router/` and confirm it predates this plan's commits.) Expected: empty
output.

- [ ] **Step 5: End-to-end smoke test at the gRPC layer (no Go yet)**

Start the server and confirm it answers a real `FindRoute` call. The
simplest way without writing a throwaway client: reuse
`grpc_health_probe` for liveness (already verified in Task 4) plus a tiny
scratch C++ client compiled against the same generated stubs:

```bash
cat > /tmp/smoke_client.cpp << 'EOF'
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "chainroute/v1/routing.grpc.pb.h"

int main() {
    auto channel = grpc::CreateChannel("127.0.0.1:50098", grpc::InsecureChannelCredentials());
    auto stub = chainroute::v1::RoutingService::NewStub(channel);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->FindRoute(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << "\n";
        return 1;
    }
    std::cout << "route_found=" << response.route_found()
              << " total_fee=" << response.total_fee()
              << " hops=" << response.hops_size() << "\n";
    return 0;
}
EOF
g++ -std=c++20 -I cpp-routing-service/build/generated \
    /tmp/smoke_client.cpp \
    cpp-routing-service/build/generated/chainroute/v1/routing.pb.cc \
    cpp-routing-service/build/generated/chainroute/v1/routing.grpc.pb.cc \
    $(pkg-config --cflags --libs grpc++ protobuf 2>/dev/null || echo "-lgrpc++ -lprotobuf") \
    -o /tmp/smoke_client

./cpp-routing-service/build/chainroute_service_server --seed=1001 --listen-address=127.0.0.1:50098 &
SERVER_PID=$!
sleep 1
/tmp/smoke_client
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
rm -f /tmp/smoke_client.cpp /tmp/smoke_client
```

Expected: prints a line like `route_found=1 total_fee=<some number>
hops=<some count>`, confirming the full path (gRPC client → server →
`RoutingServiceImpl` → `NetworkSimulator`/`findCheapestRoute` → response)
works. If `pkg-config` isn't available or doesn't find grpc++/protobuf,
link flags may need adjusting for this environment (e.g. explicit `-L`/
`-I` paths from `brew --prefix grpc`/`brew --prefix protobuf`) — this
scratch client is exploratory tooling, adjust freely; it is not committed.

- [ ] **Step 6: Commit (only if Step 2 or Step 3 required code changes)**

```bash
git add -A
git commit -m "fix(service): resolve issues found during Plan A verification"
```

If no changes were needed, skip this commit.

---

**End of Plan A.** Plan B (`docs/superpowers/plans/2026-09-10-grpc-service-plan-b-go.md`)
implements the Go API service and the real Go → gRPC → C++ end-to-end
test, building on this plan's `routing.proto` and running
`chainroute_service_server` as a subprocess.
