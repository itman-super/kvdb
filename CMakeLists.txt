cmake_minimum_required(VERSION 3.16)

project(kvdb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

add_executable(kvdb
        src/main.cpp
        src/kv_store.cpp
        src/data_file.cpp
)

target_include_directories(kvdb
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(kvdb PRIVATE /W4)
else()
    target_compile_options(kvdb PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(kvdb_test
        tests/kv_store_test.cpp
        src/kv_store.cpp
        src/data_file.cpp

)

target_include_directories(kvdb_test
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(kvdb_test PRIVATE /W4)
else()
    target_compile_options(kvdb_test PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(kvdb_bench
        tests/kv_store_benchmark.cpp
        src/kv_store.cpp
        src/data_file.cpp
)

target_include_directories(kvdb_bench
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(kvdb_bench PRIVATE /W4)
else()
    target_compile_options(kvdb_bench PRIVATE -Wall -Wextra -Wpedantic)
endif()
