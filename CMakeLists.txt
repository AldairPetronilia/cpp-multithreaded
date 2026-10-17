cmake_minimum_required(VERSION 3.20)

project(event_driven_trading_poc VERSION 0.2.0 LANGUAGES CXX)

option(TRADING_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

# Benchmarks and latency figures are meaningless unoptimised, so default to Release.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

# Portable standard C++: messaging, workers, API manager, strategies and telemetry.
add_library(trading_core
    src/api.cpp
    src/components.cpp
    src/config.cpp
    src/gateway_pool.cpp
    src/journal.cpp
    src/messaging.cpp
    src/metrics.cpp
    src/replay.cpp
    src/resilience.cpp
    src/session.cpp
    src/strategies.cpp
    src/system.cpp
    src/telemetry.cpp
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

set(trading_targets trading_core)

# The live dashboard needs POSIX sockets, so it is only built where they exist.
if(UNIX)
    set(page_bytes ${CMAKE_CURRENT_BINARY_DIR}/generated/dashboard_page.inc)
    add_custom_command(
        OUTPUT ${page_bytes}
        COMMAND ${CMAKE_COMMAND} -DINPUT=${CMAKE_CURRENT_SOURCE_DIR}/app/dashboard.html
                -DOUTPUT=${page_bytes} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/embed_page.cmake
        DEPENDS app/dashboard.html cmake/embed_page.cmake
        COMMENT "Embedding app/dashboard.html"
        VERBATIM
    )
    add_library(trading_dashboard src/dashboard_server.cpp src/dashboard_page.cpp ${page_bytes})
    target_include_directories(trading_dashboard PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated)
    target_link_libraries(trading_dashboard PUBLIC trading_core)
    list(APPEND trading_targets trading_dashboard)
endif()

add_executable(trading_demo app/main.cpp)
add_executable(trading_benchmark bench/benchmark.cpp)
add_executable(trading_tests tests/tests.cpp tests/resilience_tests.cpp)
target_include_directories(trading_tests PRIVATE tests)
foreach(target trading_demo trading_benchmark trading_tests)
    if(TARGET trading_dashboard)
        target_link_libraries(${target} PRIVATE trading_dashboard)
        target_compile_definitions(${target} PRIVATE TRADING_HAS_DASHBOARD=1)
    else()
        target_link_libraries(${target} PRIVATE trading_core)
        target_compile_definitions(${target} PRIVATE TRADING_HAS_DASHBOARD=0)
    endif()
endforeach()
list(APPEND trading_targets trading_demo trading_benchmark trading_tests)

enable_testing()
add_test(NAME trading_tests COMMAND trading_tests)
add_test(NAME demo_smoke COMMAND trading_demo --events 1500 --interval-us 200)

# Record a supervised run with two gateways, then replay exactly that recording in a
# separate process: the journal on disk is all the replay has. The replay exits non-zero if
# the signals it derives do not match the recorded ones.
set(smoke_journal ${CMAKE_BINARY_DIR}/smoke-journal)
add_test(NAME smoke_journal_clean COMMAND ${CMAKE_COMMAND} -E remove_directory ${smoke_journal})
add_test(NAME demo_smoke_record
         COMMAND trading_demo --events 3000 --interval-us 200 --supervise --gateways 2
                 --journal ${smoke_journal} --session smoke)
add_test(NAME demo_smoke_replay
         COMMAND trading_demo --replay ${smoke_journal}/smoke --replay-speed max)
add_test(NAME demo_smoke_playback
         COMMAND trading_demo --replay ${smoke_journal}/smoke --replay-mode playback --replay-speed max)
set_tests_properties(smoke_journal_clean PROPERTIES FIXTURES_SETUP smoke_clean)
set_tests_properties(demo_smoke_record PROPERTIES FIXTURES_REQUIRED smoke_clean FIXTURES_SETUP smoke_recorded)
set_tests_properties(demo_smoke_replay demo_smoke_playback PROPERTIES FIXTURES_REQUIRED smoke_recorded)

foreach(target IN LISTS trading_targets)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
    endif()
endforeach()
