add_executable(adsl_test_mds_predictor mds/test_predictor.cc)
target_link_libraries(adsl_test_mds_predictor mds global)

add_executable(test_thread test.cpp)
target_link_libraries(test_thread common)

add_executable(adsl_test_mutex common/test_mutex.cc)
target_link_libraries(adsl_test_mutex common)

add_executable(adsl_test_cond_wait common/test_cond_wait.cc)
target_link_libraries(adsl_test_cond_wait common)
