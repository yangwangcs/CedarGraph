# Cypher Query Engine Tests

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

# Parser Test
add_executable(test_parser
    test_parser.cc
)
target_link_libraries(test_parser cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_parser)

# Fingerprint Test
add_executable(test_fingerprint
    test_fingerprint.cc
)
target_link_libraries(test_fingerprint cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_fingerprint)

# Plan Cache Test
add_executable(test_plan_cache
    test_plan_cache.cc
)
target_link_libraries(test_plan_cache cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_plan_cache)
