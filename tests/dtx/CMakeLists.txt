# DTX Unit Tests
# =============================================================================

add_executable(test_embedded_raft_rpc dtx/unit/test_embedded_raft_rpc.cc)
target_link_libraries(test_embedded_raft_rpc PRIVATE
    cedar
    cedar_graph
    cedar_cypher
    ${GTEST_MAIN_TARGET}
    pthread
)
gtest_discover_tests(test_embedded_raft_rpc)
