# Predicate Pushdown Test
add_executable(test_predicate_pushdown
    test_predicate_pushdown.cc
)
target_link_libraries(test_predicate_pushdown cedar_cypher gtest gtest_main)
add_test(NAME test_predicate_pushdown COMMAND test_predicate_pushdown)
# Cypher Query Engine Tests

# WHERE Clause Test
add_executable(test_where_clause
    test_where_clause.cc
)
target_link_libraries(test_where_clause cedar_cypher gtest gtest_main)
add_test(NAME test_where_clause COMMAND test_where_clause)

# Parameterized Query Test
add_executable(test_parameterized_query
    test_parameterized_queries.cc
)
target_link_libraries(test_parameterized_query cedar_cypher gtest gtest_main)
add_test(NAME test_parameterized_query COMMAND test_parameterized_query)

# Parser Test
add_executable(test_parser
    test_parser.cc
)
target_link_libraries(test_parser cedar_cypher gtest gtest_main)
add_test(NAME test_parser COMMAND test_parser)

# Fingerprint Test
add_executable(test_fingerprint
    test_fingerprint.cc
)
target_link_libraries(test_fingerprint cedar_cypher gtest gtest_main)
add_test(NAME test_fingerprint COMMAND test_fingerprint)

# Plan Cache Test
add_executable(test_plan_cache
    test_plan_cache.cc
)
target_link_libraries(test_plan_cache cedar_cypher gtest gtest_main)
add_test(NAME test_plan_cache COMMAND test_plan_cache)

# IndexScan Test
add_executable(test_index_scan
    test_index_scan.cc
)
target_link_libraries(test_index_scan cedar_cypher gtest gtest_main)
add_test(NAME test_index_scan COMMAND test_index_scan)

# EXPLAIN Output Test
add_executable(test_explain_output
    test_explain_output.cc
)
target_link_libraries(test_explain_output cedar_cypher gtest gtest_main)
add_test(NAME test_explain_output COMMAND test_explain_output)

add_executable(test_profile_output
    test_profile_output.cc
)
target_link_libraries(test_profile_output cedar_cypher gtest gtest_main)
add_test(NAME test_profile_output COMMAND test_profile_output)

# Variable-Length Expand Test
add_executable(test_variable_length_expand
    test_variable_length_expand.cc
)
target_link_libraries(test_variable_length_expand cedar_cypher gtest gtest_main)
add_test(NAME test_variable_length_expand COMMAND test_variable_length_expand)

# Execution Operators Test
add_executable(test_execution_operators
    test_execution_operators.cc
)
target_link_libraries(test_execution_operators cedar_cypher gtest gtest_main)
add_test(NAME test_execution_operators COMMAND test_execution_operators)
