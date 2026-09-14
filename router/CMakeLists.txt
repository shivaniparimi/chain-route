cmake_minimum_required(VERSION 3.20)
project(chainroute_router CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(chainroute STATIC
    src/chain.cpp
    src/asset.cpp
    src/node.cpp
    src/edge.cpp
    src/graph.cpp
)
target_include_directories(chainroute PUBLIC include)

if (NOT MSVC)
    target_compile_options(chainroute PUBLIC -Wall -Wextra -Wpedantic)
endif()

enable_testing()
add_subdirectory(tests)
