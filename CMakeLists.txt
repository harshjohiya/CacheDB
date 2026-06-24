cmake_minimum_required(VERSION 3.15)
project(mini-redis VERSION 1.0 DESCRIPTION "A multithreaded C++20 Redis Clone")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Disables compiler-specific extensions for portability

add_executable(mini-redis 
    src/main.cpp 
    src/server.cpp
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(mini-redis PRIVATE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(mini-redis PRIVATE 
        -Wall 
        -Wextra 
        -Wpedantic 
        -Werror      # Fails the build if there is even one warning
        -Wconversion # Warns on implicit type conversions (e.g., int to short)
    )
elseif(MSVC)
    target_compile_options(mini-redis PRIVATE /W4 /WX)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose Release or Debug" FORCE)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug" AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(mini-redis PRIVATE -fsanitize=address -fsanitize=undefined -g)
    target_link_options(mini-redis PRIVATE -fsanitize=address -fsanitize=undefined)
endif()