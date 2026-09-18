cmake_minimum_required(VERSION 3.20)
project(chainroute_router CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Shares the CHAINROUTE_BUILD_TESTS cache variable with cpp-routing-service's
# top-level CMakeLists.txt (which add_subdirectory()s this project and sets
# the option before doing so). option() leaves an existing cache entry's
# value untouched, so cpp-routing-service's -DCHAINROUTE_BUILD_TESTS=OFF
# also skips this project's own network-dependent googletest FetchContent
# (tests/CMakeLists.txt) when built as that subdirectory -- not just
# cpp-routing-service/tests -- while standalone builds of this project
# (e.g. the CI "cpp" job's `cmake -S router -B router/build`) still default
# to building and running the router's own tests.
option(CHAINROUTE_BUILD_TESTS "Build the chainroute_router test suite" ON)

add_library(chainroute STATIC
    src/chain.cpp
    src/asset.cpp
    src/node.cpp
    src/edge.cpp
    src/graph.cpp
    src/route.cpp
    src/sim/deterministic_rng.cpp
    src/sim/network_simulator.cpp
)
target_include_directories(chainroute PUBLIC include)

if (NOT MSVC)
    target_compile_options(chainroute PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(CHAINROUTE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
