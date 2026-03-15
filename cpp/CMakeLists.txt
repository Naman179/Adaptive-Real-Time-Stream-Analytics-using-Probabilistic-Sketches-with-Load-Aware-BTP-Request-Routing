cmake_minimum_required(VERSION 3.21)
project(btp_cpp_worker CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimisation flags for the release build
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(
    "$<$<CONFIG:Release>:-O3;-march=native>"
    "$<$<CONFIG:Debug>:-g;-O0;-fsanitize=address>"
)
add_link_options(
    "$<$<CONFIG:Debug>:-fsanitize=address>"
)

# ---------------------------------------------------------------------------
# Dependencies — fetched automatically via CMake FetchContent
# ---------------------------------------------------------------------------

include(FetchContent)

# xxHash — ultra-fast non-cryptographic hashing
FetchContent_Declare(
    xxhash
    GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
    GIT_TAG        v0.8.2
    SOURCE_SUBDIR  cmake_unofficial
)
FetchContent_MakeAvailable(xxhash)

# nlohmann/json — header-only JSON library
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# redis-plus-plus (depends on hiredis)
FetchContent_Declare(
    hiredis
    GIT_REPOSITORY https://github.com/redis/hiredis.git
    GIT_TAG        v1.2.0
)
FetchContent_MakeAvailable(hiredis)

FetchContent_Declare(
    redis_plus_plus
    GIT_REPOSITORY https://github.com/sewenew/redis-plus-plus.git
    GIT_TAG        1.3.12
)
FetchContent_MakeAvailable(redis_plus_plus)

# Catch2 — C++ unit testing framework
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.6.0
)
FetchContent_MakeAvailable(Catch2)

# ---------------------------------------------------------------------------
# librdkafka — system-installed on Linux/Mac (docker image installs it)
# Install instructions: https://github.com/confluentinc/librdkafka#installation
# ---------------------------------------------------------------------------

find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

# ---------------------------------------------------------------------------
# Shared include paths (used by both worker and tests)
# ---------------------------------------------------------------------------

set(SKETCH_INCLUDES
    ${CMAKE_SOURCE_DIR}/sketches
    ${CMAKE_SOURCE_DIR}/worker
)

# ---------------------------------------------------------------------------
# Analytics Worker executable
# ---------------------------------------------------------------------------

add_executable(btp_worker
    worker/worker.cpp
)

target_include_directories(btp_worker PRIVATE
    ${SKETCH_INCLUDES}
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(btp_worker PRIVATE
    xxhash
    nlohmann_json::nlohmann_json
    redis++
    hiredis
    ${RDKAFKA_LIBRARIES}
)

target_compile_options(btp_worker PRIVATE ${RDKAFKA_CFLAGS_OTHER})

# ---------------------------------------------------------------------------
# Unit tests (Catch2)
# ---------------------------------------------------------------------------

enable_testing()

add_executable(test_sketches
    tests/test_sketches.cpp
)

target_include_directories(test_sketches PRIVATE
    ${SKETCH_INCLUDES}
)

target_link_libraries(test_sketches PRIVATE
    xxhash
    Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(test_sketches)
