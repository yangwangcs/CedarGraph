# Examples CMakeLists.txt

# Simple example
add_executable(simple_example simple_example.cc)
target_link_libraries(simple_example cedar cedar_graph)

# DTx example
add_executable(dtx_example dtx_example.cc)
target_link_libraries(dtx_example cedar cedar_graph)
if(TARGET gRPC::grpc++)
    target_link_libraries(dtx_example gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(dtx_example ${GRPC_LIBRARIES})
endif()

# Multi-Raft example
add_executable(multi_raft_example multi_raft_example.cc)
target_link_libraries(multi_raft_example cedar cedar_graph)
if(TARGET gRPC::grpc++)
    target_link_libraries(multi_raft_example gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(multi_raft_example ${GRPC_LIBRARIES})
endif()

# Failover Demo - EventBus + HealthMonitor + FailoverManager
add_executable(failover_demo failover_demo.cc)
target_link_libraries(failover_demo cedar cedar_graph)
if(TARGET gRPC::grpc++)
    target_link_libraries(failover_demo gRPC::grpc++)
elseif(GRPC_FOUND)
    target_link_libraries(failover_demo ${GRPC_LIBRARIES})
endif()
