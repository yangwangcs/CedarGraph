include(GoogleTest)

# Prevent flaky timeouts on slow performance tests under parallel ctest
set(CTEST_TEST_TIMEOUT 60)

if(GTest_FOUND)
    set(GTEST_TARGET GTest::gtest)
    set(GTEST_MAIN_TARGET GTest::gtest_main)
else()
    set(GTEST_TARGET gtest)
    set(GTEST_MAIN_TARGET gtest_main)
endif()

set(CEDAR_TEST_LIBS
    cedar
    cedar_graph
    cedar_cypher
    ${GTEST_MAIN_TARGET}
    pthread
)

# Add gRPC libraries if DTX is enabled
if(TARGET gRPC::grpc++)
    list(APPEND CEDAR_TEST_LIBS gRPC::grpc++)
elseif(GRPC_FOUND)
    list(APPEND CEDAR_TEST_LIBS ${GRPC_LIBRARIES})
endif()

# Helper macro to add tests
macro(add_cedar_test TEST_NAME)
    add_executable(${TEST_NAME} ${TEST_NAME}.cc)
    target_link_libraries(${TEST_NAME} ${CEDAR_TEST_LIBS})
    gtest_discover_tests(${TEST_NAME})
endmacro()

# Cypher tests
add_subdirectory(cypher)

# Queryd tests
add_executable(test_subquery_execution queryd/test_subquery_execution.cc)
target_link_libraries(test_subquery_execution ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_subquery_execution)

# Core tests
add_cedar_test(test_cedar_key)
add_cedar_test(test_descriptor)
add_cedar_test(test_descriptor_schema_version)
add_executable(test_cedar_graph_storage storage/test_cedar_graph_storage.cc)
target_link_libraries(test_cedar_graph_storage ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_graph_storage)
add_cedar_test(test_cedar_update)
add_cedar_test(test_cedar_update_validation)
add_cedar_test(test_cedar_update_e2e)
add_cedar_test(test_cedar_update_simple_e2e)
add_cedar_test(test_cedar_update_persistence)
# add_cedar_test(test_cedar_basic_persistence)
add_cedar_test(test_cedar_key_roundtrip)
# add_cedar_test(test_cedar_scan)
# add_cedar_test(test_cedar_scan_simple)
# add_cedar_test(test_cedar_scan_view)
add_cedar_test(test_cedar_key_enhanced)
# add_cedar_test(test_compaction_merger_v2)
# add_cedar_test(test_skeleton_cache)
# add_cedar_test(test_skeleton_cache_lsm_integration)
add_cedar_test(test_temporal_minimal)
# add_cedar_test(test_cedarscan_crash)
# add_cedar_test(test_write_perf_only)
# add_cedar_test(test_skeleton_cache_bare_perf)
# add_cedar_test(test_rw_performance)
add_cedar_test(test_driver_transaction)

# End-to-end partition test
add_executable(test_end_to_end_partition test_end_to_end_partition.cc)
target_link_libraries(test_end_to_end_partition ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_end_to_end_partition)

# DTX tests (in subdirectories)
add_executable(test_2pc_recovery dtx/test_2pc_recovery.cpp)
target_link_libraries(test_2pc_recovery ${CEDAR_TEST_LIBS} glog)
gtest_discover_tests(test_2pc_recovery)

add_executable(test_meta_service dtx/unit/test_meta_service.cc)
target_link_libraries(test_meta_service ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service)

add_executable(test_meta_service_grpc_client dtx/unit/test_meta_service_grpc_client.cc)
target_link_libraries(test_meta_service_grpc_client ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service_grpc_client)

add_executable(test_meta_service_grpc dtx/test_meta_service_grpc.cc)
target_link_libraries(test_meta_service_grpc ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service_grpc)

add_executable(test_partition_index_build dtx/test_partition_index_build.cc)
target_link_libraries(test_partition_index_build ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_index_build)

add_executable(test_partition_migration_end_to_end dtx/test_partition_migration_end_to_end.cc)
target_link_libraries(test_partition_migration_end_to_end ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_migration_end_to_end)

# add_cedar_test(test_sst_capacity_analysis)
# add_cedar_test(test_auto_compaction_file_based)
# add_cedar_test(test_sst_structure_analysis)
# add_cedar_test(test_sstv2_production)
# add_cedar_test(test_sstv2_integration)
# add_cedar_test(test_large_sst)
# add_cedar_test(test_small_file_compaction)

# Governance tests
add_executable(test_service_registry governance/test_service_registry.cc)
target_link_libraries(test_service_registry ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_service_registry)

add_executable(test_config_manager governance/test_config_manager.cc)
target_link_libraries(test_config_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_config_manager)

add_executable(test_health_checker governance/test_health_checker.cc)
target_link_libraries(test_health_checker ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_health_checker)

# Integration tests
add_executable(test_event_bus integration/test_event_bus.cc)
target_link_libraries(test_event_bus ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_event_bus)

add_executable(test_governance_integration integration/test_governance_integration.cc)
target_link_libraries(test_governance_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_governance_integration)

add_executable(test_full_stack integration/test_full_stack.cc)
target_link_libraries(test_full_stack ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_full_stack)

# End-to-end tests
add_executable(test_distributed_storage_api end_to_end/test_distributed_storage_api.cc)
target_link_libraries(test_distributed_storage_api ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_distributed_storage_api)

# Cluster tests
add_executable(test_distributed_crud cluster/test_distributed_crud.cc)
target_link_libraries(test_distributed_crud ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_distributed_crud)

add_executable(test_distributed_batch cluster/test_distributed_batch.cc)
target_link_libraries(test_distributed_batch ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_distributed_batch)

add_executable(test_cluster_connection cluster/test_cluster_connection.cc)
target_link_libraries(test_cluster_connection ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cluster_connection)

add_executable(test_failover cluster/test_failover.cc)
target_link_libraries(test_failover ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_failover)

add_executable(test_health_monitoring cluster/test_health_monitoring.cc)
target_link_libraries(test_health_monitoring ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_health_monitoring)

add_executable(test_eventbus_integration cluster/test_eventbus_integration.cc)
target_link_libraries(test_eventbus_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_eventbus_integration)

add_executable(test_consistent_hash cluster/test_consistent_hash.cc)
target_link_libraries(test_consistent_hash ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_consistent_hash)

# add_executable(test_storage_integration cluster/test_storage_integration.cc)
# target_link_libraries(test_storage_integration ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_storage_integration)

add_executable(test_cross_dc_replication cluster/test_cross_dc_replication.cc)
target_link_libraries(test_cross_dc_replication ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cross_dc_replication)

# NEW: Partition Raft Tests
# add_executable(test_partition_raft cluster/test_partition_raft.cc)
# target_link_libraries(test_partition_raft ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_raft)

add_executable(test_raft_propose_batch cluster/test_raft_propose_batch.cc)
target_link_libraries(test_raft_propose_batch ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_raft_propose_batch)

# Cluster test tool
add_executable(cluster_test_tool ../tools/cluster_test_tool.cc)
target_link_libraries(cluster_test_tool cedar cedar_graph ${GTEST_MAIN_TARGET} pthread)
if(TARGET gRPC::grpc++)
    target_link_libraries(cluster_test_tool gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(cluster_test_tool ${GRPC_LIBRARIES})
endif()

# Partition Raft Manager Tests
# add_executable(test_partition_raft_manager cluster/test_partition_raft_manager.cc)
# target_link_libraries(test_partition_raft_manager ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_raft_manager)

# Partition Router Tests
add_executable(test_partition_router cluster/test_partition_router.cc)
target_link_libraries(test_partition_router ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_router)

# Partition Storage Integration Tests
# add_executable(test_partition_storage_integration cluster/test_partition_storage_integration.cc)
# target_link_libraries(test_partition_storage_integration ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_storage_integration)

# Partition Metadata Service Tests
# add_executable(test_partition_metadata_service cluster/test_partition_metadata_service.cc)
# target_link_libraries(test_partition_metadata_service ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_metadata_service)

# add_executable(test_partition_router_leader_only test_partition_router_leader_only.cc)
# target_link_directories(test_partition_router_leader_only PRIVATE /opt/homebrew/lib)
# target_link_libraries(test_partition_router_leader_only ${CEDAR_TEST_LIBS} cedar_queryd)
# gtest_discover_tests(test_partition_router_leader_only)

# add_executable(test_cypher_validator test_cypher_validator.cc)
# target_link_directories(test_cypher_validator PRIVATE /opt/homebrew/lib)
# target_link_libraries(test_cypher_validator ${CEDAR_TEST_LIBS} cedar_queryd)
# gtest_discover_tests(test_cypher_validator)

# Storage Interface Tests
# add_executable(test_storage_interface test_storage_interface.cc)
# target_link_libraries(test_storage_interface ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_storage_interface)

# add_executable(test_storage_interface_predicate test_storage_interface_predicate.cc)
# target_link_libraries(test_storage_interface_predicate ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_storage_interface_predicate)

# GCN tests
add_executable(test_tmv_edge gcn/test_tmv_edge.cc)
target_link_libraries(test_tmv_edge cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tmv_edge)

add_executable(test_tmv_chunk gcn/test_tmv_chunk.cc)
target_link_libraries(test_tmv_chunk cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tmv_chunk)

add_executable(test_numa_arena gcn/test_numa_arena.cc)
target_link_libraries(test_numa_arena cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_numa_arena)

add_executable(test_tmv_index gcn/test_tmv_index.cc)
target_link_libraries(test_tmv_index cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tmv_index)

add_executable(test_tmv_engine gcn/test_tmv_engine.cc)
target_link_libraries(test_tmv_engine cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tmv_engine)

add_executable(test_gcn_service gcn/test_gcn_service.cc)
target_link_libraries(test_gcn_service cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_gcn_service PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_gcn_service)

add_executable(test_query_dispatcher gcn/test_query_dispatcher.cc)
target_link_directories(test_query_dispatcher PRIVATE /opt/homebrew/lib)
target_link_libraries(test_query_dispatcher cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_dispatcher)

add_executable(test_coordinator_client gcn/test_coordinator_client.cc)
target_link_directories(test_coordinator_client PRIVATE /opt/homebrew/lib)
target_link_libraries(test_coordinator_client cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_coordinator_client)

add_executable(test_scatter_gather_router gcn/test_scatter_gather_router.cc)
target_link_directories(test_scatter_gather_router PRIVATE /opt/homebrew/lib)
target_link_libraries(test_scatter_gather_router cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_scatter_gather_router)

add_executable(test_local_compute_thread gcn/test_local_compute_thread.cc)
target_link_directories(test_local_compute_thread PRIVATE /opt/homebrew/lib)
target_link_libraries(test_local_compute_thread cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_local_compute_thread)

add_executable(test_event_applier gcn/test_event_applier.cc)
target_link_directories(test_event_applier PRIVATE /opt/homebrew/lib)
target_link_libraries(test_event_applier cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_event_applier)

add_executable(test_watermark_gc gcn/test_watermark_gc.cc)
target_link_directories(test_watermark_gc PRIVATE /opt/homebrew/lib)
target_link_libraries(test_watermark_gc cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_watermark_gc)

add_executable(test_gcn_event_stream gcn/test_gcn_event_stream.cc)
target_link_libraries(test_gcn_event_stream cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_gcn_event_stream PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_gcn_event_stream)

add_executable(test_storage_extensions gcn/test_storage_extensions.cc)
target_link_libraries(test_storage_extensions ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_extensions)

add_executable(test_cypher_gcn_routing gcn/test_cypher_gcn_routing.cc)
target_link_libraries(test_cypher_gcn_routing cedar_cypher cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_cypher_gcn_routing PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_cypher_gcn_routing)

add_executable(test_end_to_end gcn/test_end_to_end.cc)
target_link_libraries(test_end_to_end cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_end_to_end PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_end_to_end)

add_executable(test_storage_backfill gcn/test_storage_backfill.cc)
target_link_libraries(test_storage_backfill cedar_gcn cedar_graph ${GTEST_MAIN_TARGET})
target_link_directories(test_storage_backfill PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_storage_backfill)

add_executable(test_graph_service_router
    service/test_graph_service_router.cc
    ../src/service/graph_service_router.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.grpc.pb.cc
)
target_link_libraries(test_graph_service_router cedar cedar_cypher cedar_queryd gRPC::grpc++ ${GTEST_MAIN_TARGET})
target_link_directories(test_graph_service_router PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_graph_service_router)
