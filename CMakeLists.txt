cmake_minimum_required(VERSION 3.18)
project(async_spatial_routing VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler flags ──────────────────────────────────────────────────────────
add_compile_options(
    -O3
    -march=native
    -Wall
    -Wextra
    -Wpedantic
    -fno-exceptions          # hard ban on exception overhead in hot path
    -fno-rtti
)

# ── Dependencies ─────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)

# Google Benchmark (expects installed via apt / vcpkg / conan)
find_package(benchmark REQUIRED)

# ── Library target ────────────────────────────────────────────────────────────
add_library(spatial_routing STATIC
    src/graph_engine.cpp
    src/telemetry_node.cpp
)

target_include_directories(spatial_routing
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(spatial_routing
    PUBLIC
        Threads::Threads
)

# ── Benchmark executable ──────────────────────────────────────────────────────
add_executable(latency_bench benchmarks/latency_bench.cpp)

target_link_libraries(latency_bench
    PRIVATE
        spatial_routing
        benchmark::benchmark
        benchmark::benchmark_main
        Threads::Threads
)

# ── Optional: unit test stub ──────────────────────────────────────────────────
option(BUILD_TESTS "Build unit tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
