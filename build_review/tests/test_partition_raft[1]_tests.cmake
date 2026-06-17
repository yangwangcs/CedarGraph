add_test([=[PartitionRaftStub.CompileAndLink]=]  /Users/wangyang/Desktop/CedarGraph-Core/build_review/tests/test_partition_raft [==[--gtest_filter=PartitionRaftStub.CompileAndLink]==] --gtest_also_run_disabled_tests)
set_tests_properties([=[PartitionRaftStub.CompileAndLink]=]  PROPERTIES DEF_SOURCE_LINE /Users/wangyang/Desktop/CedarGraph-Core/tests/cluster/test_partition_raft.cc:6 WORKING_DIRECTORY /Users/wangyang/Desktop/CedarGraph-Core/build_review/tests SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set(  test_partition_raft_TESTS PartitionRaftStub.CompileAndLink)
