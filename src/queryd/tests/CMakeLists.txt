# Copyright (c) 2025 The Cedar Authors. All rights reserved.
# Tests for cedar-queryd

# Test executables
add_executable(test_distributed_executor
    test_distributed_executor.cpp
)

target_include_directories(test_distributed_executor PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(test_distributed_executor
    cedar_queryd
    gtest
    gtest_main
    gmock
    pthread
)

add_executable(test_storage_client
    test_storage_client.cpp
)

target_include_directories(test_storage_client PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(test_storage_client
    cedar_queryd
    gtest
    gtest_main
    gmock
    pthread
)

add_executable(test_meta_client
    test_meta_client.cpp
)

target_include_directories(test_meta_client PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(test_meta_client
    cedar_queryd
    gtest
    gtest_main
    gmock
    pthread
)

# Register tests
add_test(NAME distributed_executor_test COMMAND test_distributed_executor)
add_test(NAME storage_client_test COMMAND test_storage_client)
add_test(NAME meta_client_test COMMAND test_meta_client)

# Set test properties
set_tests_properties(distributed_executor_test PROPERTIES
    LABELS "queryd"
    TIMEOUT 60
)

set_tests_properties(storage_client_test PROPERTIES
    LABELS "queryd"
    TIMEOUT 60
)

set_tests_properties(meta_client_test PROPERTIES
    LABELS "queryd"
    TIMEOUT 60
)
