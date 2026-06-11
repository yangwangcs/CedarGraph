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

# SET/DELETE parsing test
add_executable(test_set_delete_parsing
    cypher/test_set_delete_parsing.cc
)
target_link_libraries(test_set_delete_parsing cedar cedar_cypher cedar_queryd gtest pthread)
gtest_discover_tests(test_set_delete_parsing)

# Write operators test
add_executable(test_write_operators
    cypher/test_write_operators.cc
)
target_link_libraries(test_write_operators cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_write_operators)

# MERGE/WITH/UNWIND parsing test
add_executable(test_merge_with_unwind_parsing
    cypher/test_merge_with_unwind_parsing.cc
)
target_link_libraries(test_merge_with_unwind_parsing cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_merge_with_unwind_parsing)

# MERGE/WITH/UNWIND execution test
add_executable(test_merge_with_unwind_execution
    cypher/test_merge_with_unwind_execution.cc
)
target_link_libraries(test_merge_with_unwind_execution cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_merge_with_unwind_execution)

# End-to-end Cypher execution test (MERGE / UNWIND / WITH)
add_executable(test_cypher_execution
    cypher/test_cypher_execution.cc
)
target_link_libraries(test_cypher_execution cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_cypher_execution)
# Queryd tests
add_executable(test_subquery_execution queryd/test_subquery_execution.cc)
target_link_libraries(test_subquery_execution ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_subquery_execution)

# Core tests
add_cedar_test(test_status)
add_cedar_test(test_cedar_key)
add_cedar_test(test_descriptor)
add_cedar_test(test_descriptor_schema_version)
add_cedar_test(test_env)
add_executable(test_env_posix core/test_env_posix.cc)
target_link_libraries(test_env_posix ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_env_posix)
add_executable(test_compaction db/test_compaction.cc)
target_link_libraries(test_compaction ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_compaction)

add_executable(test_compaction_streaming db/test_compaction_streaming.cc)
target_link_libraries(test_compaction_streaming ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_compaction_streaming)

add_executable(test_manifest_replay db/test_manifest_replay.cc)
target_link_libraries(test_manifest_replay ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_manifest_replay)

add_executable(test_tombstone_flush db/test_tombstone_flush.cc)
target_link_libraries(test_tombstone_flush ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_tombstone_flush)

add_executable(test_manifest_compact db/test_manifest_compact.cc)
target_link_libraries(test_manifest_compact ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_manifest_compact)

add_executable(test_repair_db db/test_repair_db.cc)
target_link_libraries(test_repair_db ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_repair_db)

add_executable(test_memtable_iterator_race db/test_memtable_iterator_race.cc)
target_link_libraries(test_memtable_iterator_race ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_memtable_iterator_race)

add_executable(test_recursive_lock db/test_recursive_lock.cc)
target_link_libraries(test_recursive_lock ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_recursive_lock)

add_executable(test_cedar_graph_storage storage/test_cedar_graph_storage.cc)
target_link_libraries(test_cedar_graph_storage ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_graph_storage)

add_executable(test_edge_atomicity storage/test_edge_atomicity.cc)
target_link_libraries(test_edge_atomicity ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_edge_atomicity)

add_executable(test_temporal_constraint storage/test_temporal_constraint.cc)
target_link_libraries(test_temporal_constraint ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_temporal_constraint)

add_executable(test_failover_races storage/test_failover_races.cc)
target_link_libraries(test_failover_races ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_failover_races)

add_executable(lsm_engine_lifecycle_test storage/lsm_engine_lifecycle_test.cc)
target_link_libraries(lsm_engine_lifecycle_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(lsm_engine_lifecycle_test)

add_executable(test_lsm_engine_index storage/test_lsm_engine_index.cc)
target_link_libraries(test_lsm_engine_index ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_lsm_engine_index)

add_executable(test_accumulated_buffer_race storage/test_accumulated_buffer_race.cc)
target_link_libraries(test_accumulated_buffer_race ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_accumulated_buffer_race)
add_cedar_test(test_cedar_update)
add_cedar_test(test_cedar_update_validation)
add_cedar_test(test_cedar_update_e2e)
add_cedar_test(test_cedar_update_simple_e2e)
add_cedar_test(test_cedar_update_persistence)
add_executable(test_cedar_basic_persistence storage/test_cedar_basic_persistence.cc)
target_link_libraries(test_cedar_basic_persistence ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_basic_persistence)
add_cedar_test(test_cedar_key_roundtrip)

# Security blocker tests
add_executable(test_security_blockers security/test_security_blockers.cc)
target_link_libraries(test_security_blockers ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_security_blockers)

# JWT parser tests
add_executable(jwt_parser_test dtx/security/jwt_parser_test.cc)
target_link_libraries(jwt_parser_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(jwt_parser_test)

# JWT JSON parser tests (nlohmann/json)
add_executable(test_jwt_json_parser dtx/test_jwt_json_parser.cc)
target_link_libraries(test_jwt_json_parser ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_jwt_json_parser)

# RBAC glob/exact matching tests
add_executable(rbac_test dtx/security/rbac_test.cc)
target_link_libraries(rbac_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(rbac_test)

# Audit log path restriction tests
add_executable(audit_log_test dtx/security/audit_log_test.cc)
target_link_libraries(audit_log_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(audit_log_test)

# SecurityManager audit log short path test
add_executable(test_security_manager dtx/test_security_manager.cc)
target_link_libraries(test_security_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_security_manager)

# Supporting modules critical batch
add_executable(test_supporting_critical security/test_supporting_critical.cc)
target_link_libraries(test_supporting_critical ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_supporting_critical)

add_cedar_test(test_cedar_scan)
add_cedar_test(test_cedar_scan_simple)
add_cedar_test(test_cedar_scan_view)
add_cedar_test(test_cedar_key_enhanced)
add_cedar_test(test_compaction_merger_v2)
add_cedar_test(test_skeleton_cache)
add_cedar_test(test_skeleton_cache_lsm_integration)

# P1-6: build the full skeleton cache test suite (storage/test_skeleton_cache.cc)
add_executable(test_storage_skeleton_cache storage/test_skeleton_cache.cc)
target_link_libraries(test_storage_skeleton_cache ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_skeleton_cache)
add_cedar_test(test_temporal_minimal)
add_cedar_test(test_cedarscan_crash)
# add_cedar_test(test_write_perf_only)      # perf test — keep disabled in CI
# add_cedar_test(test_skeleton_cache_bare_perf)  # perf test — keep disabled in CI
# add_cedar_test(test_rw_performance)       # perf test — keep disabled in CI
add_cedar_test(test_driver_transaction)

# End-to-end partition test
add_executable(test_end_to_end_partition test_end_to_end_partition.cc)
target_link_libraries(test_end_to_end_partition ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_end_to_end_partition)

# Streaming partitioner thread safety test
add_executable(test_streaming_partitioner_thread_safety partition/test_streaming_partitioner_thread_safety.cc)
target_link_libraries(test_streaming_partitioner_thread_safety ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_streaming_partitioner_thread_safety)

# Multi-threaded concurrent end-to-end test
add_executable(test_multi_threaded_concurrent_e2e test_multi_threaded_concurrent_e2e.cc)
target_link_libraries(test_multi_threaded_concurrent_e2e ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_multi_threaded_concurrent_e2e)

# DTX tests (in subdirectories)
add_executable(test_2pc_recovery dtx/test_2pc_recovery.cpp)
target_link_libraries(test_2pc_recovery ${CEDAR_TEST_LIBS})
add_test(NAME test_2pc_recovery COMMAND test_2pc_recovery)

add_executable(test_meta_service dtx/unit/test_meta_service.cc)
target_link_libraries(test_meta_service ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service)

add_executable(test_meta_service_grpc_client dtx/unit/test_meta_service_grpc_client.cc)
target_link_libraries(test_meta_service_grpc_client ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service_grpc_client)

# DTX unit orphan tests — restored
add_executable(test_bookmark_manager dtx/unit/test_bookmark_manager.cc)
target_link_libraries(test_bookmark_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_bookmark_manager)

add_executable(test_lnd_occ dtx/unit/test_lnd_occ.cc)
target_link_libraries(test_lnd_occ ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_lnd_occ)

add_executable(test_temporal_window dtx/unit/test_temporal_window.cc)
target_link_libraries(test_temporal_window ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_temporal_window)

add_executable(test_twcd_engine dtx/unit/test_twcd_engine.cc)
target_link_libraries(test_twcd_engine ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_twcd_engine)

add_executable(test_version_chain dtx/unit/test_version_chain.cc)
target_link_libraries(test_version_chain ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_version_chain)

add_executable(test_deadlock_detector dtx/unit/test_deadlock_detector.cc)
target_link_libraries(test_deadlock_detector ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_deadlock_detector)

add_executable(test_hybrid_logical_clock_unit dtx/unit/test_hybrid_logical_clock.cc)
target_link_libraries(test_hybrid_logical_clock_unit ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_hybrid_logical_clock_unit)

add_executable(test_dtx_integration dtx/unit/test_integration.cc)
target_link_libraries(test_dtx_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_dtx_integration)

add_executable(test_partition_unit dtx/unit/test_partition.cc)
target_link_libraries(test_partition_unit ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_unit)

add_executable(test_storage_server_unit dtx/unit/test_storage_server.cc)
target_link_libraries(test_storage_server_unit ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_server_unit)

add_executable(test_meta_service_grpc dtx/test_meta_service_grpc.cc)
target_link_libraries(test_meta_service_grpc ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service_grpc)

add_executable(test_partition_index_build dtx/test_partition_index_build.cc)
target_link_libraries(test_partition_index_build ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_index_build)

add_executable(test_partition_migration_end_to_end dtx/test_partition_migration_end_to_end.cc)
target_link_libraries(test_partition_migration_end_to_end ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_migration_end_to_end)

add_executable(test_migration_checksum dtx/test_migration_checksum.cc)
target_link_libraries(test_migration_checksum ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_migration_checksum)

add_executable(test_migration_checksum_verify dtx/test_migration_checksum_verify.cc)
target_link_libraries(test_migration_checksum_verify ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_migration_checksum_verify)

add_executable(test_migration_wal_catchup dtx/test_migration_wal_catchup.cc)
target_link_libraries(test_migration_wal_catchup ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_migration_wal_catchup)

add_executable(test_transaction_pool transaction/test_transaction_pool.cc)
target_link_libraries(test_transaction_pool ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_transaction_pool)

add_executable(test_wal_ordering transaction/test_wal_ordering.cc)
target_link_libraries(test_wal_ordering ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_wal_ordering)

add_executable(wal_future_test transaction/wal_future_test.cc)
target_link_libraries(wal_future_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(wal_future_test)

add_executable(test_pool_state_reset transaction/test_pool_state_reset.cc)
target_link_libraries(test_pool_state_reset ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_pool_state_reset)

add_executable(test_commit_serializability transaction/test_commit_serializability.cc)
target_link_libraries(test_commit_serializability ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_commit_serializability)

add_executable(test_batchwrite_memtable_refresh transaction/test_batchwrite_memtable_refresh.cc)
target_link_libraries(test_batchwrite_memtable_refresh ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_batchwrite_memtable_refresh)

add_executable(test_plan_cache_thread_safety cypher/test_plan_cache_thread_safety.cc)
target_link_libraries(test_plan_cache_thread_safety ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_plan_cache_thread_safety)

add_executable(test_concurrent_plan_execution cypher/test_concurrent_plan_execution.cc)
target_link_libraries(test_concurrent_plan_execution ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_concurrent_plan_execution)

add_executable(test_value_type_safety cypher/test_value_type_safety.cc)
target_link_libraries(test_value_type_safety ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_value_type_safety)

add_executable(test_execution_context_validation cypher/test_execution_context_validation.cc)
target_link_libraries(test_execution_context_validation ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_execution_context_validation)

add_executable(test_expression_null_safety cypher/test_expression_null_safety.cc)
target_link_libraries(test_expression_null_safety ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_expression_null_safety)

add_executable(test_nodescan_real_data cypher/test_nodescan_real_data.cc)
target_link_libraries(test_nodescan_real_data ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_nodescan_real_data)

add_executable(test_planner_signature
    cypher/test_planner_signature.cc
    ../src/cypher/planner.cc
)
target_link_libraries(test_planner_signature ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_planner_signature)

add_executable(test_planner
    cypher/test_planner.cc
    ../src/cypher/planner.cc
)
target_link_libraries(test_planner ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_planner)

add_executable(test_distinct_no_collision cypher/test_distinct_no_collision.cc)
target_link_libraries(test_distinct_no_collision ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_distinct_no_collision)

add_executable(test_storage_service_impl dtx/test_storage_service_impl.cc)
target_link_libraries(test_storage_service_impl ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_storage_service_impl)

# Storage Service auth tests (P0-5)
add_executable(test_storage_service_auth dtx/test_storage_service_auth.cc)
target_link_libraries(test_storage_service_auth ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_storage_service_auth)

# Performance/structure tests — keep disabled in CI (slow or require specific setup)
# add_executable(test_sst_capacity_analysis storage/test_sst_capacity_analysis.cc)
# add_executable(test_auto_compaction_file_based storage/test_auto_compaction_file_based.cc)
# add_executable(test_sst_structure_analysis storage/test_sst_structure_analysis.cc)
add_executable(test_sstv2_production storage/test_sstv2_production.cc)
target_link_libraries(test_sstv2_production ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_sstv2_production)
# add_executable(test_sstv2_integration storage/test_sstv2_integration.cc)
# add_executable(test_large_sst storage/test_large_sst.cc)
# add_executable(test_small_file_compaction storage/test_small_file_compaction.cc)

add_executable(test_zone_columnar_reader sst/test_zone_columnar_reader.cc)
target_link_libraries(test_zone_columnar_reader ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_zone_columnar_reader)

# Governance tests
add_executable(test_service_registry governance/test_service_registry.cc)
target_link_libraries(test_service_registry ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_service_registry)

add_executable(test_service_registry_exception_safety governance/test_service_registry_exception_safety.cc)
target_link_libraries(test_service_registry_exception_safety ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_service_registry_exception_safety)

add_executable(test_config_manager governance/test_config_manager.cc)
target_link_libraries(test_config_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_config_manager)

add_executable(test_health_checker governance/test_health_checker.cc)
target_link_libraries(test_health_checker ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_health_checker)

# P1-3: HealthChecker uses bounded ThreadPool instead of std::async
add_executable(test_health_checker_thread_pool governance/test_health_checker_thread_pool.cc)
target_link_libraries(test_health_checker_thread_pool ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_health_checker_thread_pool)

add_executable(health_checker_probe_test governance/health_checker_probe_test.cc)
target_link_libraries(health_checker_probe_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(health_checker_probe_test)

add_executable(tls_config_test governance/tls_config_test.cc)
target_link_libraries(tls_config_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(tls_config_test)

# Integration tests
add_executable(test_storaged_tls_enforced integration/test_storaged_tls_enforced.cc)
target_link_libraries(test_storaged_tls_enforced ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storaged_tls_enforced)

add_executable(test_event_bus integration/test_event_bus.cc)
target_link_libraries(test_event_bus ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_event_bus)

add_executable(test_governance_integration integration/test_governance_integration.cc)
target_link_libraries(test_governance_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_governance_integration)

add_executable(test_full_stack integration/test_full_stack.cc)
target_link_libraries(test_full_stack ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_full_stack)

# Linear end-to-end pipeline test
add_executable(test_linear_end_to_end_pipeline test_linear_end_to_end_pipeline.cc)
target_link_libraries(test_linear_end_to_end_pipeline ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_linear_end_to_end_pipeline)

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

add_executable(test_in_neighbors graph/test_in_neighbors.cc)
target_link_libraries(test_in_neighbors ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_in_neighbors)

add_executable(test_out_neighbors_edge_type graph/test_out_neighbors_edge_type.cc)
target_link_libraries(test_out_neighbors_edge_type ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_out_neighbors_edge_type)

add_executable(test_pushdown_predicate graph/test_pushdown_predicate.cc)
target_link_libraries(test_pushdown_predicate ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_pushdown_predicate)

add_executable(test_cypher_api graph/test_cypher_api.cc)
target_link_libraries(test_cypher_api ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_cypher_api)

add_executable(test_graph_cypher_critical_batch
    cypher/test_graph_cypher_critical_batch.cc
)
target_link_libraries(test_graph_cypher_critical_batch ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_graph_cypher_critical_batch)

add_executable(test_cluster_connection cluster/test_cluster_connection.cc)
target_link_libraries(test_cluster_connection ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cluster_connection)

add_executable(test_rate_limiter_race queryd/test_rate_limiter_race.cc)
target_link_libraries(test_rate_limiter_race ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_rate_limiter_race)

add_executable(test_partition_routing_consistency queryd/test_partition_routing_consistency.cc)
target_link_libraries(test_partition_routing_consistency ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_partition_routing_consistency)

add_executable(test_meta_registration
    queryd/test_meta_registration.cc
    ../src/queryd/meta_client.cpp
    ${CMAKE_BINARY_DIR}/generated_proto/meta_service.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/meta_service.grpc.pb.cc
)
target_link_libraries(test_meta_registration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_registration)

add_executable(test_2pc_thread_pool dtx/test_2pc_thread_pool.cc)
target_link_libraries(test_2pc_thread_pool ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_2pc_thread_pool)

add_executable(test_2pc_pipeline_race dtx/test_2pc_pipeline_race.cc)
target_link_libraries(test_2pc_pipeline_race ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_2pc_pipeline_race)

add_executable(two_pc_atomicity_test dtx/two_pc_atomicity_test.cc)
target_link_libraries(two_pc_atomicity_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(two_pc_atomicity_test)

add_executable(decision_log_recovery_test dtx/decision_log_recovery_test.cc)
target_link_libraries(decision_log_recovery_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(decision_log_recovery_test)

add_executable(test_failover_thread_bound dtx/test_failover_thread_bound.cc)
target_link_libraries(test_failover_thread_bound ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_failover_thread_bound)

add_executable(failover_consensus_test dtx/failover_consensus_test.cc)
target_link_libraries(failover_consensus_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(failover_consensus_test)

add_executable(test_rpc_thread_bound dtx/test_rpc_thread_bound.cc)
target_link_libraries(test_rpc_thread_bound ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_rpc_thread_bound)

add_executable(test_partial_commit_rollback dtx/test_partial_commit_rollback.cc)
target_link_libraries(test_partial_commit_rollback ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_partial_commit_rollback)

add_executable(test_rollback_replication dtx/test_rollback_replication.cc)
target_link_libraries(test_rollback_replication ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_rollback_replication)

add_executable(test_dtx_raft_critical dtx/test_dtx_raft_critical.cc)
target_link_libraries(test_dtx_raft_critical ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_dtx_raft_critical)

add_executable(test_register_participant dtx/test_register_participant.cc)
target_link_libraries(test_register_participant ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_register_participant)

# P1-1: participant registry cleanup on Commit/Abort
add_executable(test_participant_registry_cleanup dtx/test_participant_registry_cleanup.cc)
target_link_libraries(test_participant_registry_cleanup ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_participant_registry_cleanup)

# P1-2: DTX RPC client promise handling OOM safety
add_executable(test_dtx_rpc_client_promise_safety dtx/test_dtx_rpc_client_promise_safety.cc)
target_link_libraries(test_dtx_rpc_client_promise_safety ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_dtx_rpc_client_promise_safety)

# P1-5: ServiceDiscovery DNS use-after-free fix
add_executable(test_service_discovery_dns_safety dtx/test_service_discovery_dns_safety.cc)
target_link_libraries(test_service_discovery_dns_safety ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_service_discovery_dns_safety)

add_executable(test_cluster_initializer_registration dtx/test_cluster_initializer_registration.cc)
target_link_libraries(test_cluster_initializer_registration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cluster_initializer_registration)

add_executable(test_braft_read_index dtx/test_braft_read_index.cc)
target_link_libraries(test_braft_read_index ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_braft_read_index)

add_executable(test_tls_fail_secure dtx/test_tls_fail_secure.cc)
target_link_libraries(test_tls_fail_secure ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_tls_fail_secure)

add_executable(test_storaged_raft_stub dtx/test_storaged_raft_stub.cc)
target_link_libraries(test_storaged_raft_stub ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storaged_raft_stub)

add_executable(test_storaged_raft_snapshot dtx/test_storaged_raft_snapshot.cc)
target_link_libraries(test_storaged_raft_snapshot ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storaged_raft_snapshot)

add_executable(test_hybrid_logical_clock dtx/test_hybrid_logical_clock.cc)
target_link_libraries(test_hybrid_logical_clock ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_hybrid_logical_clock)

add_executable(test_metrics_registry metrics/test_metrics_registry.cc)
target_link_libraries(test_metrics_registry ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_metrics_registry)

add_executable(test_metrics_labels metrics/test_metrics_labels.cc)
target_link_libraries(test_metrics_labels ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_metrics_labels)

add_executable(prometheus_escape_test metrics/prometheus_escape_test.cc)
target_link_libraries(prometheus_escape_test ${CEDAR_TEST_LIBS})
gtest_discover_tests(prometheus_escape_test)

add_executable(test_failover cluster/test_failover.cc)
target_link_libraries(test_failover ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_failover)

add_executable(test_failover_health_score cluster/test_failover_health_score.cc)
target_link_libraries(test_failover_health_score ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_failover_health_score)

add_executable(test_health_monitoring cluster/test_health_monitoring.cc)
target_link_libraries(test_health_monitoring ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_health_monitoring)

add_executable(test_eventbus_integration cluster/test_eventbus_integration.cc)
target_link_libraries(test_eventbus_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_eventbus_integration)

add_executable(test_consistent_hash cluster/test_consistent_hash.cc)
target_link_libraries(test_consistent_hash ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_consistent_hash)

add_executable(test_storage_integration cluster/test_storage_integration.cc)
target_link_libraries(test_storage_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_integration)

add_executable(test_corrupt_log_recovery raft/test_corrupt_log_recovery.cc)
target_link_libraries(test_corrupt_log_recovery ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_corrupt_log_recovery)

add_executable(test_cross_dc_replication cluster/test_cross_dc_replication.cc)
target_link_libraries(test_cross_dc_replication ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cross_dc_replication)

# NEW: Partition Raft Tests
add_executable(test_partition_raft cluster/test_partition_raft.cc)
target_link_libraries(test_partition_raft ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_raft)



# Cluster test tool
add_executable(cluster_test_tool ../tools/cluster_test_tool.cc)
target_link_libraries(cluster_test_tool cedar cedar_graph ${GTEST_MAIN_TARGET} pthread)
if(TARGET gRPC::grpc++)
    target_link_libraries(cluster_test_tool gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(cluster_test_tool ${GRPC_LIBRARIES})
endif()

# Partition Raft Manager Tests
add_executable(test_partition_raft_manager cluster/test_partition_raft_manager.cc)
target_link_libraries(test_partition_raft_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_raft_manager)

# Partition Router Tests
add_executable(test_partition_router cluster/test_partition_router.cc)
target_link_libraries(test_partition_router ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_router)

# Partition Storage Integration Tests
add_executable(test_partition_storage_integration cluster/test_partition_storage_integration.cc)
target_link_libraries(test_partition_storage_integration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_storage_integration)

# Partition Metadata Service Tests
add_executable(test_partition_metadata_service cluster/test_partition_metadata_service.cc)
target_link_libraries(test_partition_metadata_service ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_metadata_service)

add_executable(test_partition_router_leader_only test_partition_router_leader_only.cc)
target_link_directories(test_partition_router_leader_only PRIVATE /opt/homebrew/lib)
target_link_libraries(test_partition_router_leader_only ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_partition_router_leader_only)

add_executable(test_cypher_validator test_cypher_validator.cc)
target_link_directories(test_cypher_validator PRIVATE /opt/homebrew/lib)
target_link_libraries(test_cypher_validator ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_cypher_validator)

# Storage Subquery Tests
add_executable(test_storage_subquery storage/test_storage_subquery.cc)
target_link_libraries(test_storage_subquery ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_subquery)

add_executable(test_parallel_compaction_scheduler storage/test_parallel_compaction_scheduler.cc)
target_link_libraries(test_parallel_compaction_scheduler ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_parallel_compaction_scheduler)

add_executable(test_storage_critical_batch storage/test_storage_critical_batch.cc)
target_link_libraries(test_storage_critical_batch ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_critical_batch)

# Storage Interface Tests
add_executable(test_storage_interface test_storage_interface.cc)
target_link_libraries(test_storage_interface ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_interface)

add_executable(test_cedar_config storage/test_cedar_config.cc)
target_link_libraries(test_cedar_config ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_config)

add_executable(test_storage_interface_predicate test_storage_interface_predicate.cc)
target_link_libraries(test_storage_interface_predicate ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_storage_interface_predicate)

# Coordinator tests
add_executable(test_location_table coordinator/test_location_table.cc)
target_link_libraries(test_location_table ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_location_table)

# DTX GCN cache handler tests
add_executable(test_meta_service_gcn_cache dtx/test_meta_service_gcn_cache.cc)
target_link_libraries(test_meta_service_gcn_cache ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_meta_service_gcn_cache)

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

add_executable(test_tmv_engine_safety gcn/test_tmv_engine_safety.cc)
target_link_libraries(test_tmv_engine_safety cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tmv_engine_safety)

add_executable(test_gcn_service gcn/test_gcn_service.cc)
target_link_libraries(test_gcn_service cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_gcn_service PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_gcn_service)

add_executable(test_adaptive_execution queryd/test_adaptive_execution.cc)
target_link_libraries(test_adaptive_execution ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_adaptive_execution)

add_executable(test_queryd_blockers queryd/test_queryd_blockers.cc)
target_link_libraries(test_queryd_blockers ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_queryd_blockers)


add_executable(test_queryd_critical queryd/test_queryd_critical.cc)
target_link_libraries(test_queryd_critical ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_queryd_critical)

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

add_executable(test_backpressure_controller gcn/test_backpressure_controller.cc)
target_link_directories(test_backpressure_controller PRIVATE /opt/homebrew/lib)
target_link_libraries(test_backpressure_controller cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_backpressure_controller)

add_executable(test_local_compute_thread gcn/test_local_compute_thread.cc)
target_link_directories(test_local_compute_thread PRIVATE /opt/homebrew/lib)
target_link_libraries(test_local_compute_thread cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_local_compute_thread)

add_executable(test_event_applier gcn/test_event_applier.cc)
target_link_directories(test_event_applier PRIVATE /opt/homebrew/lib)
target_link_libraries(test_event_applier cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_event_applier)

add_executable(event_applier_backpressure_test gcn/event_applier_backpressure_test.cc)
target_link_directories(event_applier_backpressure_test PRIVATE /opt/homebrew/lib)
target_link_libraries(event_applier_backpressure_test cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(event_applier_backpressure_test)

add_executable(event_applier_cv_test gcn/event_applier_cv_test.cc)
target_link_directories(event_applier_cv_test PRIVATE /opt/homebrew/lib)
target_link_libraries(event_applier_cv_test cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(event_applier_cv_test)

add_executable(test_watermark_gc gcn/test_watermark_gc.cc)
target_link_directories(test_watermark_gc PRIVATE /opt/homebrew/lib)
target_link_libraries(test_watermark_gc cedar_gcn ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_watermark_gc)

add_executable(test_gcn_event_stream gcn/test_gcn_event_stream.cc)
target_link_libraries(test_gcn_event_stream cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_gcn_event_stream PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_gcn_event_stream)

add_executable(test_gcn_governance_critical gcn/test_gcn_governance_critical.cc)
target_link_libraries(test_gcn_governance_critical cedar_gcn ${GTEST_MAIN_TARGET})
target_link_directories(test_gcn_governance_critical PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_gcn_governance_critical)

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

add_executable(test_graph_service_router_auth
    service/test_graph_service_router_auth.cc
    ../src/service/graph_service_router.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.grpc.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/query_service.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/query_service.grpc.pb.cc
)
target_link_libraries(test_graph_service_router_auth cedar cedar_cypher cedar_queryd gRPC::grpc++ ${GTEST_MAIN_TARGET})
target_link_directories(test_graph_service_router_auth PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_graph_service_router_auth)

add_executable(test_migration_pipeline
    service/test_migration_pipeline.cc
)
target_link_libraries(test_migration_pipeline cedar cedar_graph gRPC::grpc++ ${GTEST_MAIN_TARGET} pthread)
target_link_directories(test_migration_pipeline PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_migration_pipeline)



add_executable(test_service_meta_critical
    service/test_service_meta_critical.cc
    ../src/service/graph_service_router.cc
    ../src/dtx/grpc/meta_service_grpc.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.grpc.pb.cc
)
target_link_libraries(test_service_meta_critical cedar cedar_graph cedar_cypher cedar_queryd gRPC::grpc++ ${GTEST_MAIN_TARGET} pthread)
target_link_directories(test_service_meta_critical PRIVATE /opt/homebrew/lib)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_service_meta_critical PRIVATE -Wno-vla-cxx-extension)
endif()
add_test(NAME test_service_meta_critical COMMAND test_service_meta_critical)

add_executable(test_2pc_cache_atomicity
  end_to_end/test_2pc_cache_atomicity.cc
)
target_link_libraries(test_2pc_cache_atomicity
  ${CEDAR_TEST_LIBS} cedar_queryd
)
add_test(NAME test_2pc_cache_atomicity COMMAND test_2pc_cache_atomicity)

# Phase C: Critical Untested Modules
add_executable(test_active_entity_bitmap storage/test_active_entity_bitmap.cc)
target_link_libraries(test_active_entity_bitmap ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_active_entity_bitmap)

add_executable(test_block_cache storage/test_block_cache.cc)
target_link_libraries(test_block_cache ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_block_cache)

add_executable(test_versioned_skiplist_lockfree storage/test_versioned_skiplist_lockfree.cc)
target_link_libraries(test_versioned_skiplist_lockfree ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_versioned_skiplist_lockfree)

add_executable(test_mth_partitioner partition/test_mth_partitioner.cc)
target_link_libraries(test_mth_partitioner ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_mth_partitioner)

add_executable(test_partition_strategy_manager partition/test_partition_strategy_manager.cc)
target_link_libraries(test_partition_strategy_manager ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_strategy_manager)

add_executable(test_bloom_filter sst/test_bloom_filter.cc)
target_link_libraries(test_bloom_filter ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_bloom_filter)

add_executable(test_column_coders sst/test_column_coders.cc)
target_link_libraries(test_column_coders ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_column_coders)

add_executable(test_full_pipeline end_to_end/test_full_pipeline.cc)
target_link_libraries(test_full_pipeline ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_full_pipeline)
