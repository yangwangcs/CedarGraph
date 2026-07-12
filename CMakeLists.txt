cmake_minimum_required(VERSION 3.14)
project(cedar VERSION 0.1.0 LANGUAGES CXX)

# macOS: work around undefined symbols from vendored brpc (profiler/jemalloc stubs)
if(APPLE)
    add_link_options(
        "-Wl,-U,__Z13GetStackTracePPvii"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop"
        "-Wl,-U,_mallctl"
        "-Wl,-U,_malloc_stats_print"
        "-Wl,-no_warn_duplicate_libraries"
    )
    # Global link directory for Homebrew libraries (gflags, grpc, etc.)
    link_directories(/opt/homebrew/lib)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# macOS: Fix C++ standard library headers path when CommandLineTools usr/include is incomplete
if(APPLE)
    execute_process(COMMAND xcrun --sdk macosx --show-sdk-path
                    OUTPUT_VARIABLE OSX_SDK_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(OSX_CXX_V1 "${OSX_SDK_PATH}/usr/include/c++/v1")
    if(EXISTS "${OSX_CXX_V1}/cstdint" AND NOT EXISTS "/usr/include/c++/v1/cstdint")
        add_compile_options(-isystem "${OSX_CXX_V1}")
    endif()
endif()

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(/opt/homebrew/include)  # gRPC headers on macOS
include_directories(/opt/homebrew/opt/nlohmann-json/include)  # nlohmann/json

# Proto output directory (must be defined early)
set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR}/generated_proto)
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})
include_directories(${PROTO_OUT_DIR})  # For generated protobuf files

find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# Generate protobuf files
set(PROTO_FILES 
    ${CMAKE_SOURCE_DIR}/proto/cedar_graph.proto
    ${CMAKE_SOURCE_DIR}/proto/raft.proto
    ${CMAKE_SOURCE_DIR}/proto/raft_service.proto
    ${CMAKE_SOURCE_DIR}/proto/metad_admin.proto
    ${CMAKE_SOURCE_DIR}/proto/meta_service.proto
    ${CMAKE_SOURCE_DIR}/proto/query_service.proto
    ${CMAKE_SOURCE_DIR}/proto/migration_service.proto
    ${CMAKE_SOURCE_DIR}/proto/storage_service.proto
    ${CMAKE_SOURCE_DIR}/proto/dtx_protocol.proto
    ${CMAKE_SOURCE_DIR}/proto/gcn_service.proto
    ${CMAKE_SOURCE_DIR}/proto/cdc_service.proto
)

add_custom_command(
    OUTPUT ${PROTO_OUT_DIR}/cedar_graph.pb.cc ${PROTO_OUT_DIR}/cedar_graph.pb.h
           ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.cc ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.h
           ${PROTO_OUT_DIR}/raft.pb.cc ${PROTO_OUT_DIR}/raft.pb.h
           ${PROTO_OUT_DIR}/raft.grpc.pb.cc ${PROTO_OUT_DIR}/raft.grpc.pb.h
           ${PROTO_OUT_DIR}/raft_service.pb.cc ${PROTO_OUT_DIR}/raft_service.pb.h
           ${PROTO_OUT_DIR}/raft_service.grpc.pb.cc ${PROTO_OUT_DIR}/raft_service.grpc.pb.h
           ${PROTO_OUT_DIR}/metad_admin.pb.cc ${PROTO_OUT_DIR}/metad_admin.pb.h
           ${PROTO_OUT_DIR}/metad_admin.grpc.pb.cc ${PROTO_OUT_DIR}/metad_admin.grpc.pb.h
           ${PROTO_OUT_DIR}/meta_service.pb.cc ${PROTO_OUT_DIR}/meta_service.pb.h
           ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc ${PROTO_OUT_DIR}/meta_service.grpc.pb.h
           ${PROTO_OUT_DIR}/query_service.pb.cc ${PROTO_OUT_DIR}/query_service.pb.h
           ${PROTO_OUT_DIR}/query_service.grpc.pb.cc ${PROTO_OUT_DIR}/query_service.grpc.pb.h
           ${PROTO_OUT_DIR}/storage_service.pb.cc ${PROTO_OUT_DIR}/storage_service.pb.h
           ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc ${PROTO_OUT_DIR}/storage_service.grpc.pb.h
           ${PROTO_OUT_DIR}/migration_service.pb.cc ${PROTO_OUT_DIR}/migration_service.pb.h
           ${PROTO_OUT_DIR}/migration_service.grpc.pb.cc ${PROTO_OUT_DIR}/migration_service.grpc.pb.h
           ${PROTO_OUT_DIR}/dtx_protocol.pb.cc ${PROTO_OUT_DIR}/dtx_protocol.pb.h
           ${PROTO_OUT_DIR}/dtx_protocol.grpc.pb.cc ${PROTO_OUT_DIR}/dtx_protocol.grpc.pb.h
           ${PROTO_OUT_DIR}/gcn_service.pb.cc ${PROTO_OUT_DIR}/gcn_service.pb.h
           ${PROTO_OUT_DIR}/gcn_service.grpc.pb.cc ${PROTO_OUT_DIR}/gcn_service.grpc.pb.h
           ${PROTO_OUT_DIR}/cdc_service.pb.cc ${PROTO_OUT_DIR}/cdc_service.pb.h
           ${PROTO_OUT_DIR}/cdc_service.grpc.pb.cc ${PROTO_OUT_DIR}/cdc_service.grpc.pb.h
    COMMAND protobuf::protoc
    ARGS --grpc_out=${PROTO_OUT_DIR}
         --cpp_out=${PROTO_OUT_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         -I ${CMAKE_SOURCE_DIR}/proto
         ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf and gRPC files"
)

add_custom_target(generate_proto ALL
    DEPENDS ${PROTO_OUT_DIR}/cedar_graph.pb.cc ${PROTO_OUT_DIR}/cedar_graph.pb.h
            ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.cc ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.h
            ${PROTO_OUT_DIR}/raft.pb.cc ${PROTO_OUT_DIR}/raft.pb.h
            ${PROTO_OUT_DIR}/raft.grpc.pb.cc ${PROTO_OUT_DIR}/raft.grpc.pb.h
            ${PROTO_OUT_DIR}/metad_admin.pb.cc ${PROTO_OUT_DIR}/metad_admin.pb.h
            ${PROTO_OUT_DIR}/metad_admin.grpc.pb.cc ${PROTO_OUT_DIR}/metad_admin.grpc.pb.h
            ${PROTO_OUT_DIR}/meta_service.pb.cc ${PROTO_OUT_DIR}/meta_service.pb.h
            ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc ${PROTO_OUT_DIR}/meta_service.grpc.pb.h
            ${PROTO_OUT_DIR}/query_service.pb.cc ${PROTO_OUT_DIR}/query_service.pb.h
            ${PROTO_OUT_DIR}/query_service.grpc.pb.cc ${PROTO_OUT_DIR}/query_service.grpc.pb.h
            ${PROTO_OUT_DIR}/storage_service.pb.cc ${PROTO_OUT_DIR}/storage_service.pb.h
            ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc ${PROTO_OUT_DIR}/storage_service.grpc.pb.h
            ${PROTO_OUT_DIR}/migration_service.pb.cc ${PROTO_OUT_DIR}/migration_service.pb.h
            ${PROTO_OUT_DIR}/migration_service.grpc.pb.cc ${PROTO_OUT_DIR}/migration_service.grpc.pb.h
            ${PROTO_OUT_DIR}/dtx_protocol.pb.cc ${PROTO_OUT_DIR}/dtx_protocol.pb.h
            ${PROTO_OUT_DIR}/dtx_protocol.grpc.pb.cc ${PROTO_OUT_DIR}/dtx_protocol.grpc.pb.h
            ${PROTO_OUT_DIR}/gcn_service.pb.cc ${PROTO_OUT_DIR}/gcn_service.pb.h
            ${PROTO_OUT_DIR}/gcn_service.grpc.pb.cc ${PROTO_OUT_DIR}/gcn_service.grpc.pb.h
            ${PROTO_OUT_DIR}/cdc_service.pb.cc ${PROTO_OUT_DIR}/cdc_service.pb.h
            ${PROTO_OUT_DIR}/cdc_service.grpc.pb.cc ${PROTO_OUT_DIR}/cdc_service.grpc.pb.h
)

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED liblz4)
pkg_check_modules(ZSTD libzstd)
if(NOT ZSTD_FOUND)
  message(STATUS "Zstd not found, L3+ will use LZ4 fallback")
endif()
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(gRPC CONFIG QUIET)
if(NOT gRPC_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GRPC REQUIRED grpc++ grpc protobuf)
    find_package(Protobuf REQUIRED)
else()
    # Include gRPC directories
    include_directories(${gRPC_INCLUDE_DIRS})
endif()
if(NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ INTERFACE IMPORTED)
    set_target_properties(gRPC::grpc++ PROPERTIES
        INTERFACE_LINK_LIBRARIES "${GRPC_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIRS}"
    )
endif()
if(NOT TARGET yaml-cpp::yaml-cpp)
    if(TARGET yaml-cpp)
        add_library(yaml-cpp::yaml-cpp ALIAS yaml-cpp)
    else()
        find_library(YAML_CPP_LIBRARY NAMES yaml-cpp REQUIRED)
        add_library(yaml-cpp::yaml-cpp UNKNOWN IMPORTED)
        set_target_properties(yaml-cpp::yaml-cpp PROPERTIES
            IMPORTED_LOCATION "${YAML_CPP_LIBRARY}"
        )
    endif()
endif()

# =============================================================================
# Required: braft + brpc for production Raft consensus and RPC
# =============================================================================
# Always uses vendored versions from third_party/ for reproducible builds.
# Run ./scripts/setup_deps.sh to download dependencies.
# =============================================================================

# Verify third_party dependencies exist
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/CMakeLists.txt
   OR NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/braft/CMakeLists.txt)
    message(FATAL_ERROR 
        "third_party/brpc or third_party/braft not found.\n"
        "Please run: ./scripts/setup_deps.sh")
endif()

# Read expected versions from VERSIONS file
set(THIRD_PARTY_VERSIONS_FILE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/VERSIONS)
if(EXISTS ${THIRD_PARTY_VERSIONS_FILE})
    file(READ ${THIRD_PARTY_VERSIONS_FILE} VERSIONS_CONTENT)
    # Extract BRPC_VERSION
    string(REGEX MATCH "BRPC_VERSION=([0-9.]+)" _ ${VERSIONS_CONTENT})
    set(EXPECTED_BRPC_VERSION ${CMAKE_MATCH_1})
    # Extract BRAFT_VERSION  
    string(REGEX MATCH "BRAFT_VERSION=v?([0-9.]+)" _ ${VERSIONS_CONTENT})
    set(EXPECTED_BRAFT_VERSION ${CMAKE_MATCH_1})
    message(STATUS "Expected versions - brpc: ${EXPECTED_BRPC_VERSION}, braft: ${EXPECTED_BRAFT_VERSION}")
endif()

message(STATUS "Using vendored brpc + braft from third_party/")

# Build brpc first (braft depends on it)
set(WITH_GLOG OFF CACHE BOOL "" FORCE)
set(DOWNLOAD_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_UNIT_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/brpc EXCLUDE_FROM_ALL)

# Point braft to vendored brpc. Without these cache entries, braft's
# find_path/find_library may pick up Homebrew brpc/butil headers on macOS.
set(brpc_DIR ${CMAKE_BINARY_DIR}/third_party/brpc CACHE PATH "" FORCE)
set(BRPC_INCLUDE_PATH ${CMAKE_SOURCE_DIR}/third_party/brpc/src CACHE PATH "" FORCE)
set(BRPC_LIB brpc-static CACHE STRING "" FORCE)

# Build braft
set(BRAFT_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(BRAFT_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/braft EXCLUDE_FROM_ALL)
if(TARGET OBJ_LIB AND TARGET PROTO_LIB)
    add_dependencies(OBJ_LIB PROTO_LIB)
    target_include_directories(OBJ_LIB BEFORE PRIVATE
        ${CMAKE_BINARY_DIR}/third_party/brpc/output/include
    )
endif()

# Vendored brpc/braft are noisy on modern macOS SDKs. Treat them as third-party
# code so Cedar builds surface Cedar warnings instead of dependency churn.
foreach(_cedar_third_party_target IN ITEMS PROTO_LIB BUTIL_LIB SOURCES_LIB OBJ_LIB)
    if(TARGET ${_cedar_third_party_target})
        target_compile_options(${_cedar_third_party_target} PRIVATE -w)
    endif()
endforeach()

set(braft_FOUND TRUE)

message(STATUS "braft enabled for production Raft consensus")
add_definitions(-DCEDAR_WITH_BRAFT)

# Core library sources
set(CEDAR_CORE_SOURCES
    src/core/status.cc
    src/core/env_posix.cc
    src/core/threading.cc
    src/core/crc32c.cc
    src/metrics/metrics_registry.cc
    src/types/descriptor.cc
    src/types/cedar_key.cc
)

# Storage sources
set(CEDAR_STORAGE_SOURCES
    src/storage/lsm_engine.cc
    src/storage/cedar_memtable.cc
    src/storage/plan_cache.cc
    src/storage/cedar_config.cc
    src/storage/sst_reader_cache.cc
    src/storage/block_cache.cc
    src/storage/compaction_merger_v2.cc
    src/storage/vsl_memtable.cc
    src/storage/versioned_skiplist_lockfree.cc
    src/storage/async_index_builder.cc
    src/storage/cedar_graph_storage.cc
    src/storage/entity_lifecycle.cc
    src/storage/blob_gc_manager.cc
    src/storage/size_tiered_compaction.cc
    src/storage/parallel_compaction_engine.cc
    src/storage/block_level_compaction.cc
    src/storage/parallel_query_engine.cc
    src/storage/adaptive_thread_pool.cc
    src/storage/auto_blob_storage.cc
    src/storage/cedar_compaction_filter.cc
    src/storage/active_entity_bitmap.cc
    src/storage/skeleton_cache.cc
    src/storage/storage_health_monitor.cc
    src/storage/failover_manager.cc
    src/storage/consistent_hash_ring.cc
    src/storage/storage_interface.cc
    src/storage/delta_encoder.cc
    src/storage/arena_pool.cc
    src/storage/async_io.cc
    src/storage/cedar_update.cc
    src/storage/cross_partition_edge_writer.cc
)

# Query sources (CedarScan - snapshot-based temporal scan engine)
set(CEDAR_QUERY_SOURCES
    src/query/cedar_scan.cc
    src/query/query_cache.cc
)

# Driver sources (Neo4j-style transaction API)
set(CEDAR_DRIVER_SOURCES
    src/driver/bookmark.cc
    src/driver/retry_policy.cc
    src/driver/transaction_types.cc
    src/driver/session.cc
)

# DTx sources (Distributed Transaction Protocol)
set(CEDAR_DTX_SOURCES
    src/dtx/utils/temporal_window.cc
    src/dtx/utils/hybrid_logical_clock.cc
    src/dtx/coordinator/partition.cc
    src/dtx/coordinator/txn_context.cc
    src/dtx/protocol/twcd_engine.cc
    src/dtx/protocol/lsm_native_occ.cc
    src/dtx/protocol/version_chain.cc
    src/dtx/protocol/bookmark_manager.cc
    src/dtx/meta/meta_service.cc
    src/dtx/meta/meta_service_impl.cc
    src/dtx/meta/meta_types.cc
    src/dtx/raft/grpc_tls.cc
    src/dtx/raft/braft_node.cc

    src/dtx/load_balancer.cc
    src/dtx/coordinator_integration.cc
    src/dtx/unified_meta_client.cc

    src/dtx/phi_accrual.cc
    src/dtx/storage/failover_manager.cc
    src/dtx/storage/metrics_collector.cc
    src/dtx/storage/metrics_storage_impl.cc
    src/dtx/storage/partition_migrator.cc
    src/dtx/storage/braft_partition_raft.cc
    src/dtx/storage/storaged_raft_state_machine.cc
    src/dtx/storage/braft_partition_state_machine.cc
    src/dtx/storage/partition_raft_manager.cc
    src/dtx/chaos/chaos_testing.cc
    src/dtx/storage_impl/partition_storage.cc
    src/dtx/storage_impl/partition_manager.cc
    src/storage/storage_backend.cc
    src/dtx/storage_impl/storage_client.cc
    src/dtx/storage_impl/storage_server.cc
    src/dtx/storage_impl/storage_service_impl.cc
    src/dtx/storage_impl/meta_service_client.cc
    src/dtx/optimized_2pc_engine.cc
    src/dtx/chaos_testing_impl.cc
    src/dtx/storage_impl/storage_service_ext.cc
    src/dtx/storage_impl/partition_index.cc
    src/dtx/grpc/meta_service_grpc.cc
    src/dtx/dtx_rpc_client.cc
    src/dtx/transaction_state.cc
    src/dtx/transaction_timeout_manager.cc
    src/dtx/transaction_recovery_manager.cc
    src/dtx/transaction_metrics.cc
    src/dtx/deadlock_detector.cc
    src/dtx/grpc_connection_pool.cc
    src/dtx/cross_dc_replicator.cc
    src/dtx/dtx_service_impl.cc
    src/dtx/service_discovery.cc
    src/dtx/security/security_manager.cc
    src/dtx/index/alert_channels.cc
    src/dtx/grpc/migration_executor.cc
    src/dtx/dual_mode_partition_strategy.cc
    src/dtx/partition_config.cc
)

# Transaction sources
set(CEDAR_TRANSACTION_SOURCES
    src/transaction/occ_transaction.cc
    src/transaction/wal.cc
    src/transaction/batch_api.cc
    src/transaction/transaction_pool.cc
    src/transaction/batch_transaction.cc
    src/transaction/sharded_timestamp_allocator.cc
    src/transaction/wal_batch_writer.cc
    src/transaction/parallel_batch_processor.cc
    src/transaction/write_batch.cc
)

# DB sources
set(CEDAR_DB_SOURCES
    src/db/graph_db.cc
    src/db/graph_db_impl.cc
    src/db/manifest.cc
    src/db/event_bus.cc
)

# SST format sources
set(CEDAR_SST_SOURCES
    src/sst/bloom_filter.cc
    src/sst/compression.cc
    src/sst/column_coders.cc
    src/sst/blob_file_manager.cc
    src/sst/zone_encoder.cc
    src/sst/blob_file.cc
    src/sst/sst_builder_factory.cc
    src/sst/zone_columnar_builder_v2.cc
    src/sst/zone_columnar_format_v2.cc
    src/sst/zone_columnar_reader.cc
)

# Partition strategy sources (Dual-mode: StaticHash + MTHStream)
set(CEDAR_PARTITION_SOURCES
    src/partition/strategies/static_hash_strategy.cc
    src/partition/strategies/mth_stream_strategy.cc
    src/partition/partition_strategy_manager.cc
    src/partition/mth/cedar_key.cc
    src/partition/mth/temporal_sketch.cc
    src/partition/mth/partition_state.cc
    src/partition/mth/streaming_partitioner.cc
    src/partition/mth/mth_partitioner.cc
)

# Governance sources (Service Registry, ConfigManager, etc.)
set(CEDAR_GOVERNANCE_SOURCES
    src/governance/service_registry.cc
    src/governance/config_manager.cc
    src/governance/health_checker.cc
)

set(CEDAR_CDC_SOURCES
    src/cdc/partition_change_log.cc
    src/cdc/change_log_maintenance.cc
)

# Create cedar library
add_library(cedar STATIC
    ${CEDAR_CORE_SOURCES}
    ${CEDAR_STORAGE_SOURCES}
    ${CEDAR_TRANSACTION_SOURCES}
    ${CEDAR_SST_SOURCES}
    ${CEDAR_DB_SOURCES}
    ${CEDAR_QUERY_SOURCES}
    ${CEDAR_GOVERNANCE_SOURCES}
    ${CEDAR_DRIVER_SOURCES}
    ${CEDAR_DTX_SOURCES}
    ${CEDAR_PARTITION_SOURCES}
    ${CEDAR_CDC_SOURCES}
    src/service/partition_migration_service.cc
    ${PROTO_OUT_DIR}/raft_service.pb.cc
    ${PROTO_OUT_DIR}/raft_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/storage_service.pb.cc
    ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/meta_service.pb.cc
    ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/migration_service.pb.cc
    ${PROTO_OUT_DIR}/migration_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/dtx_protocol.pb.cc
    ${PROTO_OUT_DIR}/dtx_protocol.grpc.pb.cc
    ${PROTO_OUT_DIR}/cdc_service.pb.cc
    ${PROTO_OUT_DIR}/cdc_service.grpc.pb.cc
)
if(TARGET OBJ_LIB)
    add_dependencies(cedar OBJ_LIB)
endif()

target_link_libraries(cedar ${LZ4_LIBRARIES} ${ZSTD_LIBRARIES} CURL::libcurl OpenSSL::SSL OpenSSL::Crypto protobuf::libprotobuf gRPC::grpc++ yaml-cpp::yaml-cpp)
target_include_directories(cedar PUBLIC ${LZ4_INCLUDE_DIRS})
target_link_directories(cedar PUBLIC ${LZ4_LIBRARY_DIRS})
target_compile_definitions(cedar PRIVATE CEDAR_NO_GLOG)

# Link braft (always vendored)
if(TARGET braft-static)
    target_link_libraries(cedar braft-static)
elseif(TARGET braft)
    target_link_libraries(cedar braft)
else()
    message(FATAL_ERROR "braft target not found (expected braft-static or braft)")
endif()
# Add braft and brpc include directories from vendored third_party
target_include_directories(cedar SYSTEM BEFORE PUBLIC
    ${CMAKE_SOURCE_DIR}/third_party/braft/include
    ${CMAKE_BINARY_DIR}/third_party/braft/src
    ${CMAKE_BINARY_DIR}/third_party/braft
    ${CMAKE_SOURCE_DIR}/third_party/braft/src
    ${CMAKE_SOURCE_DIR}/third_party/brpc/src
    ${CMAKE_BINARY_DIR}/third_party/brpc/src
    ${CMAKE_BINARY_DIR}/third_party/brpc/output/include
)

# Graph sources
set(CEDAR_GRAPH_SOURCES
    src/graph/cedar_graph.cc
    src/graph/cedar_graph_temporal.cc
    src/graph/graph_semantic_layer.cc
    src/graph/pushdown_predicate.cc
)

# Queryd sources (Distributed Query Layer) - merged into GraphD
set(CEDAR_QUERYD_SOURCES
    src/queryd/distributed_executor.cpp
    src/queryd/query_storage_client.cpp
    src/queryd/storage_execution_context.cc
    src/queryd/meta_client.cpp
)

add_library(cedar_graph STATIC ${CEDAR_GRAPH_SOURCES})
target_link_libraries(cedar_graph cedar cedar_gcn)

# Cypher engine sources
set(CEDAR_CYPHER_SOURCES
    src/cypher/cypher_engine.cc
    src/cypher/parser.cc
    src/cypher/parser_temporal.cc
    src/cypher/execution_plan.cc
    src/cypher/expression_evaluator.cc
    src/cypher/value.cc
    src/cypher/fingerprint.cc
    src/cypher/temporal_dialect.cc
    src/cypher/cost_optimizer.cc
    src/cypher/cost_optimizer.cc
    src/cypher/operators/temporal_operators.cc
    src/cypher/operators/write_operators.cc
    src/cypher/validator.cc
)

add_library(cedar_cypher STATIC ${CEDAR_CYPHER_SOURCES})
target_link_libraries(cedar_cypher cedar cedar_graph cedar_queryd)

# Queryd 生成的 protobuf 文件
set(QUERYD_PROTO_SOURCES
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/meta_service.pb.cc
    ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc
)

# Queryd library
add_library(cedar_queryd STATIC 
    ${CEDAR_QUERYD_SOURCES}
    ${QUERYD_PROTO_SOURCES}
)
target_include_directories(cedar_queryd PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}
)
target_link_libraries(cedar_queryd 
    cedar
    gRPC::grpc++ 
    gflags 
)

# GraphD - Unified Query Router Service (merged QueryD functionality)
add_executable(graphd
    tools/graphd.cc
    src/service/graph_service_router.cc
    src/service/graphd_registrar.cc
    src/service/query_result_aggregator.cc
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/meta_service.pb.cc
    ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/storage_service.pb.cc
    ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/cedar_graph.pb.cc
    ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.cc
)
set_target_properties(graphd PROPERTIES OUTPUT_NAME "cedar-graphd")
target_include_directories(graphd PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}
)
target_link_directories(graphd PRIVATE /opt/homebrew/lib)
target_link_libraries(graphd cedar_cypher)
target_compile_definitions(graphd PRIVATE CEDAR_NO_GLOG)

# Test client for GraphD
add_executable(test_graphd_client
    tools/test_graphd_client.cc
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/meta_service.pb.cc
    ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/storage_service.pb.cc
    ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc
)
target_link_libraries(test_graphd_client gRPC::grpc++ protobuf::libprotobuf)

# Performance test tool
add_executable(test_performance
    tools/test_performance.cc
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
)
target_include_directories(test_performance PRIVATE 
    ${CMAKE_SOURCE_DIR}
)
target_link_libraries(test_performance gRPC::grpc++ protobuf::libprotobuf pthread)

# MetaD - Standalone Metadata Service (braft-based, stub until full implementation)
add_executable(metad
    tools/metad.cc
)
set_target_properties(metad PROPERTIES OUTPUT_NAME "cedar-metad")
target_link_libraries(metad cedar cedar_graph ${LZ4_LIBRARIES} gRPC::grpc++ pthread)
if(APPLE)
    target_link_directories(metad PRIVATE /opt/homebrew/lib)
endif()

# Storage server executable (with gRPC service - legacy)
add_executable(storaged_legacy
    src/dtx/storage/storage_server_with_grpc.cc
)
target_link_libraries(storaged_legacy cedar cedar_graph cedar_cypher cedar_queryd ${LZ4_LIBRARIES} gRPC::grpc++)
if(APPLE)
    target_link_directories(storaged_legacy PRIVATE /opt/homebrew/lib)
endif()

# StorageD - Standalone Storage Service (Nebula-style)
add_executable(storaged
    tools/storaged.cc
    ${PROTO_OUT_DIR}/meta_service.pb.cc
    ${PROTO_OUT_DIR}/meta_service.grpc.pb.cc
    ${PROTO_OUT_DIR}/storage_service.pb.cc
    ${PROTO_OUT_DIR}/storage_service.grpc.pb.cc
)
set_target_properties(storaged PROPERTIES OUTPUT_NAME "cedar-storaged")
target_link_libraries(storaged cedar cedar_graph cedar_cypher cedar_queryd gRPC::grpc++ pthread)
target_compile_definitions(storaged PRIVATE CEDAR_NO_GLOG)
if(APPLE)
    target_link_directories(storaged PRIVATE /opt/homebrew/lib)
endif()

# Queryd client example
add_executable(queryd_client_example
    examples/queryd/client_example.cpp
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
)
target_link_libraries(queryd_client_example gRPC::grpc++ protobuf::libprotobuf)

# 分布式写入测试
add_executable(test_distributed_write
    tests/test_distributed_write.cpp
    ${PROTO_OUT_DIR}/query_service.pb.cc
    ${PROTO_OUT_DIR}/query_service.grpc.pb.cc
)
target_link_libraries(test_distributed_write gRPC::grpc++ protobuf::libprotobuf)

# 直接存储测试
add_executable(test_storage_direct
    tests/storage/test_storage_direct.cpp
)
target_link_libraries(test_storage_direct cedar)

# 简单存储测试
add_executable(test_simple_storage
    tests/storage/test_simple_storage.cpp
)
target_link_libraries(test_simple_storage cedar)

# 调试存储测试
add_executable(test_debug_storage
    tests/test_debug_storage.cpp
)
target_link_libraries(test_debug_storage cedar)

# Property mapping test
add_executable(test_property_mapping
    tests/test_property_mapping.cpp
)
target_link_libraries(test_property_mapping cedar)

# Kind field test
add_executable(test_kind_field
    tests/test_kind_field.cpp
)
target_link_libraries(test_kind_field cedar)

# End-to-end property test
add_executable(test_e2e_property
    tests/test_e2e_property.cpp
)
target_link_libraries(test_e2e_property cedar)

# End-to-end flush test
add_executable(test_e2e_flush
    tests/test_e2e_flush.cpp
)
target_link_libraries(test_e2e_flush cedar)

# Cross-partition ScanLabel test
add_executable(test_scanlabel_cross_partition
    tests/test_scanlabel_cross_partition.cpp
)
target_link_libraries(test_scanlabel_cross_partition cedar)

# Cross-partition edge scan test
add_executable(test_edge_cross_partition
    tests/test_edge_cross_partition.cpp
)
target_link_libraries(test_edge_cross_partition cedar)

# Flush 结果测试
add_executable(test_flush_result
    tests/test_flush_result.cpp
)
target_link_libraries(test_flush_result cedar)

# Flush 详细调试测试
add_executable(test_flush_debug
    tests/test_flush_debug.cpp
)
target_link_libraries(test_flush_debug cedar)

# Flush 修复测试
add_executable(test_flush_fixed
    tests/test_flush_fixed.cpp
)
target_link_libraries(test_flush_fixed cedar)

# SST 调试测试
add_executable(test_sst_debug
    tests/storage/test_sst_debug.cpp
)
target_link_libraries(test_sst_debug cedar)

# Optional: Build examples if enabled
option(BUILD_EXAMPLES "Build example programs" OFF)
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Optional: Build tests if enabled
option(BUILD_TESTS "Build test programs" OFF)
if(BUILD_TESTS)
    enable_testing()
    find_package(GTest)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG        v1.15.2
        )
        FetchContent_MakeAvailable(googletest)
    endif()
    add_subdirectory(tests)
endif()

# Distributed Performance Test
add_executable(test_distributed_performance
    tests/test_distributed_performance.cpp
)
target_link_libraries(test_distributed_performance cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Multi-Node Performance Test (3/5/7 nodes) - gRPC version
add_executable(test_multi_node_performance
    tests/test_multi_node_performance.cpp
)
target_link_libraries(test_multi_node_performance cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Multi-Node Performance Test (3/5/7 nodes) - Local simulation
add_executable(test_multi_node_local
    tests/test_multi_node_local.cpp
)
target_link_libraries(test_multi_node_local cedar ${LZ4_LIBRARIES})

# Optimized 2PC Test (Parallel + Pipeline + Batch)
add_executable(test_2pc_optimized
    tests/test_2pc_optimized.cpp
)
target_link_libraries(test_2pc_optimized cedar ${LZ4_LIBRARIES})

# Production Configuration Demo
add_executable(test_production_config
    tests/test_production_config.cpp
)
target_link_libraries(test_production_config cedar ${LZ4_LIBRARIES})

# Stability and Recovery Test
add_executable(test_stability_recovery
    tests/test_stability_recovery.cpp
)
target_link_libraries(test_stability_recovery cedar ${LZ4_LIBRARIES})



# Docker Performance Benchmark
add_executable(test_docker_perf_benchmark
    tests/test_docker_perf_benchmark.cpp
)
target_link_libraries(test_docker_perf_benchmark cedar ${LZ4_LIBRARIES})

# Temporal Graph Performance Test
add_executable(test_temporal_graph_perf
    tests/test_temporal_graph_perf.cpp
    src/dtx/temporal_graph_benchmark.cc
)
target_link_libraries(test_temporal_graph_perf cedar ${LZ4_LIBRARIES})

# Optimized Temporal Graph Performance Test
add_executable(test_optimized_temporal_perf
    tests/test_optimized_temporal_perf.cpp
    src/dtx/temporal_graph_benchmark.cc
    src/dtx/optimized_temporal_benchmark.cc
)
target_link_libraries(test_optimized_temporal_perf cedar ${LZ4_LIBRARIES})

# Simple Distributed Verification Test
add_executable(test_distributed_simple
    tests/test_distributed_simple.cpp
)
target_link_libraries(test_distributed_simple cedar cedar_cypher cedar_queryd ${LZ4_LIBRARIES} gRPC::grpc++)

# Fixed Distributed Test
add_executable(test_distributed_fixed
    tests/test_distributed_fixed.cpp
)
target_link_libraries(test_distributed_fixed cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Debug Batch Test
add_executable(test_debug_batch
    tests/test_debug_batch.cpp
)
target_link_libraries(test_debug_batch cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Timestamp Debug Test
add_executable(test_timestamp_debug
    tests/test_timestamp_debug.cpp
)
target_link_libraries(test_timestamp_debug cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Standalone Neighbor Test
add_executable(test_neighbor_only
    tests/test_neighbor_only.cpp
)
target_link_libraries(test_neighbor_only cedar ${LZ4_LIBRARIES} gRPC::grpc++)

# Flush Persistence Test
add_executable(test_flush_persistence
    tests/test_flush_persistence.cpp
)
target_link_libraries(test_flush_persistence cedar ${LZ4_LIBRARIES} gRPC::grpc++)

add_executable(test_multilevel_compaction
    tests/test_multilevel_compaction.cpp
)
target_link_libraries(test_multilevel_compaction cedar ${LZ4_LIBRARIES} gRPC::grpc++)

add_executable(test_level_compaction
    tests/test_level_compaction.cpp
)
target_link_libraries(test_level_compaction cedar ${LZ4_LIBRARIES} gRPC::grpc++)

add_executable(test_perf_compaction_safety
    tests/test_perf_compaction_safety.cpp
)
target_link_libraries(test_perf_compaction_safety cedar ${LZ4_LIBRARIES} gRPC::grpc++)

add_executable(test_leveled_verify
    tests/test_leveled_verify.cpp
)
target_link_libraries(test_leveled_verify cedar ${LZ4_LIBRARIES} gRPC::grpc++)



# =============================================================================
# Performance Optimizations - Short-term tasks
# =============================================================================

# Real CedarGraphStorage Performance Test
add_executable(test_real_storage_perf
    tests/storage/test_real_storage_perf.cpp
)
target_link_libraries(test_real_storage_perf cedar ${LZ4_LIBRARIES})
target_compile_features(test_real_storage_perf PRIVATE cxx_std_17)

# Release Mode Compilation Settings
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -ffast-math")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG -march=native")

# DTX Governance Integration Test
add_executable(test_dtx_governance_integration
    tests/test_dtx_governance_integration.cpp
)
target_link_libraries(test_dtx_governance_integration cedar ${LZ4_LIBRARIES})

# Dual-Mode Partitioning Test (StaticHash + MTHStream)
add_executable(test_dual_mode_partitioning
    tests/test_dual_mode_partitioning.cpp
)
target_link_libraries(test_dual_mode_partitioning cedar ${LZ4_LIBRARIES})
target_include_directories(test_dual_mode_partitioning PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# Dual-Mode Partition Strategy Test (uses CedarGraph CedarKey)
add_executable(test_dual_mode_partition_strategy
    tests/test_dual_mode_partition_strategy.cpp
)
target_link_libraries(test_dual_mode_partition_strategy cedar ${LZ4_LIBRARIES})
target_include_directories(test_dual_mode_partition_strategy PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# Partition Config Test
add_executable(test_partition_config
    tests/test_partition_config.cpp
)
target_link_libraries(test_partition_config cedar ${LZ4_LIBRARIES})
target_include_directories(test_partition_config PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# Dual-Mode Integration Example
add_executable(dual_mode_integration_example
    examples/partitioning/dual_mode_integration_example.cpp
)
target_link_libraries(dual_mode_integration_example cedar ${LZ4_LIBRARIES})
target_include_directories(dual_mode_integration_example PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# MTHStream 3-Node Temporal Performance Benchmark
add_executable(test_mth_3node_temporal_performance
    tests/test_mth_3node_temporal_performance.cc
)
target_link_libraries(test_mth_3node_temporal_performance cedar ${LZ4_LIBRARIES} gRPC::grpc++ pthread)
target_include_directories(test_mth_3node_temporal_performance PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# End-to-End Partition Test (Production Readiness)
# NOTE: This target is already defined in tests/CMakeLists.txt
# add_executable(test_end_to_end_partition
#     tests/test_end_to_end_partition.cc
# )
# target_link_libraries(test_end_to_end_partition cedar ${LZ4_LIBRARIES})
# target_include_directories(test_end_to_end_partition PRIVATE 
#     ${CMAKE_SOURCE_DIR}/include
#     ${CMAKE_SOURCE_DIR}/src
# )

# Enable Release mode by default if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Release mode optimization flags: ${CMAKE_CXX_FLAGS_RELEASE}")
endif()

# Summary
# CedarGraph Storage Node - cluster node executable
add_executable(cedar_storage_node tools/cedar_storage_node.cc)
target_link_libraries(cedar_storage_node 
    cedar 
    cedar_graph 
    cedar_cypher
    pthread
)
if(TARGET gRPC::grpc++)
    target_link_libraries(cedar_storage_node gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(cedar_storage_node ${GRPC_LIBRARIES})
endif()

# CedarGraph Cluster Test Client
add_executable(cedar_cluster_client tools/cedar_cluster_client.cc)
target_link_libraries(cedar_cluster_client 
    cedar 
    cedar_graph
    pthread
)
if(APPLE)
    target_link_directories(cedar_cluster_client PRIVATE /opt/homebrew/lib)
endif()

# CedarGraph Cluster Benchmark Executable
add_executable(cedar_cluster_benchmark 
    tools/cedar_cluster_benchmark.cc
    tools/benchmark_metrics.cc
    tools/benchmark_workloads.cc
)
target_link_libraries(cedar_cluster_benchmark 
    cedar 
    cedar_graph
    pthread
)
if(APPLE)
    target_link_directories(cedar_cluster_benchmark PRIVATE /opt/homebrew/lib)
endif()
if(TARGET gRPC::grpc++)
    target_link_libraries(cedar_cluster_benchmark gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(cedar_cluster_benchmark ${GRPC_LIBRARIES})
endif()

# =============================================================================
# Client Library
# =============================================================================
set(CEDAR_CLIENT_SOURCES
    src/client/connection_pool.cc
    src/client/graphd_load_balancer.cc
    src/backup/backup_manager.cc
    src/client/service_discovery.cc
    src/client/load_balancer.cc
    src/client/query_router.cc
    src/client/session_manager.cc
    src/client/retry_handler.cc
    src/client/metrics_collector.cc
    src/client/jwt_manager.cc
    src/client/logger.cc
    src/client/config_loader.cc
    src/client/config_watcher.cc
    src/client/async_query.cc
    src/client/cluster_manager.cc
    src/client/docker_manager.cc
    src/client/k8s_manager.cc
    src/client/cluster_monitor.cc
    src/client/auto_scaler.cc
    src/client/cluster_backup.cc
    src/client/cedar_client.cc
)

add_library(cedar_client STATIC ${CEDAR_CLIENT_SOURCES})
target_link_libraries(cedar_client cedar gRPC::grpc++ OpenSSL::Crypto pthread)

# =============================================================================
# GCN (Graph Compute Node)
# =============================================================================
file(GLOB GCN_SOURCES src/gcn/*.cc)
list(FIND GCN_SOURCES "${CMAKE_SOURCE_DIR}/src/gcn/checkpoint_store.cc" GCN_CHECKPOINT_SOURCE_INDEX)
if(GCN_CHECKPOINT_SOURCE_INDEX EQUAL -1)
    list(APPEND GCN_SOURCES "${CMAKE_SOURCE_DIR}/src/gcn/checkpoint_store.cc")
endif()
list(FIND GCN_SOURCES "${CMAKE_SOURCE_DIR}/src/gcn/storage_cdc_client.cc" GCN_STORAGE_CDC_CLIENT_SOURCE_INDEX)
if(GCN_STORAGE_CDC_CLIENT_SOURCE_INDEX EQUAL -1)
    list(APPEND GCN_SOURCES "${CMAKE_SOURCE_DIR}/src/gcn/storage_cdc_client.cc")
endif()
add_library(cedar_gcn ${GCN_SOURCES}
    ${PROTO_OUT_DIR}/gcn_service.pb.cc
    ${PROTO_OUT_DIR}/gcn_service.grpc.pb.cc
)
target_link_directories(cedar_gcn PRIVATE /opt/homebrew/lib)
target_link_libraries(cedar_gcn cedar gflags)

add_executable(graphcomputenode tools/graphcomputenode.cc)
target_link_directories(graphcomputenode PRIVATE /opt/homebrew/lib)
target_link_libraries(graphcomputenode cedar_gcn cedar cedar_graph gRPC::grpc++ gflags pthread)

message(STATUS "")
message(STATUS "=== CedarGraph Build Configuration ===")
message(STATUS "Version: ${PROJECT_VERSION}")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "braft: ${braft_FOUND}")
message(STATUS "LZ4: ${LZ4_LIBRARIES}")
message(STATUS "gRPC: ${gRPC_FOUND}")
# =============================================================================
# Install targets
# =============================================================================
install(TARGETS storaged metad graphd
        RUNTIME DESTINATION bin)

install(FILES scripts/cedar-cli.sh scripts/cedar_health_monitor.sh scripts/cedar_health_check.sh scripts/run_tests.sh
        DESTINATION bin
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

message(STATUS "=======================================")


# Local concurrent memtable benchmark
add_executable(test_sharded_memtable
    tests/benchmark/test_sharded_memtable.cc
)
target_link_libraries(test_sharded_memtable cedar)

if(BUILD_TESTS)
    # GraphD Load Balancer Test
    add_executable(test_graphd_load_balancer
        tests/client/test_graphd_load_balancer.cc
    )
    target_link_libraries(test_graphd_load_balancer cedar cedar_client gtest gtest_main)
    gtest_discover_tests(test_graphd_load_balancer)
endif()
