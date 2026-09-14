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
    target_compile_options(chainroute_service_server PRIVATE -Wall -Wextra -Wpedantic)
    # Future project-code targets added here (e.g. chainroute_service_lib,
    # chainroute_service_tests) should apply the same -Wall -Wextra -Wpedantic flags.
endif()

enable_testing()
add_subdirectory(tests)
