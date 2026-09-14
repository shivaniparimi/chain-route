include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(chainroute_service_tests
    chain_asset_convert_test.cpp
    routing_service_test.cpp
)
target_link_libraries(chainroute_service_tests PRIVATE chainroute_service_lib GTest::gtest_main)
if (NOT MSVC)
    target_compile_options(chainroute_service_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

include(GoogleTest)
gtest_discover_tests(chainroute_service_tests)
