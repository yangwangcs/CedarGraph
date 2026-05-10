# Cypher Query Engine Tests

# Include main test configuration
include(${CMAKE_SOURCE_DIR}/tests/CMakeLists.txt)

# WHERE Clause Test
add_executable(test_where_clause
    test_where_clause.cc
)
target_link_libraries(test_where_clause cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_where_clause)

# Parameterized Query Test
add_executable(test_parameterized_query
    test_parameterized_queries.cc
)
target_link_libraries(test_parameterized_query cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_parameterized_query)
