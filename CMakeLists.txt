cmake_minimum_required(VERSION 3.25)
project(AegisGate VERSION 0.1.0 LANGUAGES C CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "AegisGate uses Linux epoll. Build on Linux or under WSL2.")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(AEGISGATE_SANITIZERS "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
if(AEGISGATE_SANITIZERS)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all)
    add_link_options(-fsanitize=address,undefined)
endif()

include(FetchContent)
set(LLHTTP_BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(LLHTTP_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(llhttp
    URL https://codeload.github.com/nodejs/llhttp/tar.gz/refs/tags/release/v9.4.4
    URL_HASH SHA256=5600adb1498f82a9c719b8eb19310021737388e0d92362a6d47be532710d3c96
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(llhttp)
target_compile_options(llhttp_static PRIVATE -fexceptions)
find_package(Threads REQUIRED)

add_library(aegisgate_core STATIC src/core/config.cpp src/http/message.cpp)
target_include_directories(aegisgate_core PUBLIC include)
target_link_libraries(aegisgate_core PUBLIC llhttp::llhttp)
target_compile_options(aegisgate_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

add_library(aegisgate_reactor STATIC src/network/worker.cpp)
target_link_libraries(aegisgate_reactor PUBLIC aegisgate_core Threads::Threads)
target_compile_options(aegisgate_reactor PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

add_executable(aegisgate src/core/main.cpp)
target_link_libraries(aegisgate PRIVATE aegisgate_reactor)
target_compile_options(aegisgate PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

include(CTest)
if(BUILD_TESTING)
    FetchContent_Declare(googletest
        URL https://codeload.github.com/google/googletest/tar.gz/refs/tags/v1.16.0
        URL_HASH SHA256=78c676fc63881529bf97bf9d45948d905a66833fbfa5318ea2cd7478cb98f399
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    add_executable(aegisgate_unit tests/unit/message_test.cpp tests/unit/config_test.cpp)
    target_link_libraries(aegisgate_unit PRIVATE aegisgate_core GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(aegisgate_unit DISCOVERY_MODE PRE_TEST)
    find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter)
    set(integration_suite "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/gateway_test.py")
    add_test(NAME gateway_integration COMMAND Python3::Interpreter
        "${integration_suite}" $<TARGET_FILE:aegisgate> -v)
    set_tests_properties(gateway_integration PROPERTIES TIMEOUT 90)

    add_executable(aegisgate_short_io src/core/main.cpp tests/integration/short_io.cpp)
    target_link_libraries(aegisgate_short_io PRIVATE aegisgate_reactor)
    target_link_options(aegisgate_short_io PRIVATE -Wl,--wrap=send -Wl,--wrap=recv)
    add_test(NAME gateway_short_io COMMAND Python3::Interpreter
        "${integration_suite}" $<TARGET_FILE:aegisgate_short_io> -v
        GatewayTests.test_split_headers_and_binary_echo_cross_small_buffers)
    set_tests_properties(gateway_short_io PROPERTIES
        ENVIRONMENT AEGISGATE_EXPECT_SHORT_IO=1 TIMEOUT 20)
endif()
