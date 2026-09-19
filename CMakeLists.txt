cmake_minimum_required(VERSION 3.20)

project(event_driven_trading_poc VERSION 0.1.0 LANGUAGES CXX)

option(TRADING_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

find_package(Threads REQUIRED)

add_library(trading_core
    src/api.cpp
    src/config.cpp
    src/messaging.cpp
    src/workers.cpp
)

target_include_directories(trading_core PUBLIC include)
target_compile_features(trading_core PUBLIC cxx_std_20)
target_link_libraries(trading_core PUBLIC Threads::Threads)

if(TRADING_ENABLE_TSAN)
    if(MSVC)
        message(FATAL_ERROR "ThreadSanitizer is not supported by this CMake setup on MSVC")
    endif()
    target_compile_options(trading_core PUBLIC -fsanitize=thread -fno-omit-frame-pointer -g)
    target_link_options(trading_core PUBLIC -fsanitize=thread)
endif()

add_executable(trading_demo app/main.cpp)
target_link_libraries(trading_demo PRIVATE trading_core)

add_executable(trading_benchmark bench/benchmark.cpp)
target_link_libraries(trading_benchmark PRIVATE trading_core)

enable_testing()
add_executable(trading_tests tests/tests.cpp)
target_link_libraries(trading_tests PRIVATE trading_core)
add_test(NAME trading_tests COMMAND trading_tests)

foreach(target trading_core trading_demo trading_benchmark trading_tests)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
    endif()
endforeach()
