add_test([=[CedarScanCrashStub.CompileAndLink]=]  /Users/wangyang/Desktop/CedarGraph-Core/build_review/tests/test_cedarscan_crash [==[--gtest_filter=CedarScanCrashStub.CompileAndLink]==] --gtest_also_run_disabled_tests)
set_tests_properties([=[CedarScanCrashStub.CompileAndLink]=]  PROPERTIES DEF_SOURCE_LINE /Users/wangyang/Desktop/CedarGraph-Core/tests/test_cedarscan_crash.cc:6 WORKING_DIRECTORY /Users/wangyang/Desktop/CedarGraph-Core/build_review/tests SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set(  test_cedarscan_crash_TESTS CedarScanCrashStub.CompileAndLink)
