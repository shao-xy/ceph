add_executable(adsl_test_mds_predictor mds/test_predictor.cc)
target_link_libraries(adsl_test_mds_predictor mds global)

#set(ENV{Torch_DIR} "/home/ceph/LSTM/libtorch")
#find_package(Torch REQUIRED)   
#message(STATUS "PyTorch lib found: ${TORCH_LIBRARIES}")
add_executable(adsl_test_mds_pytorch_simple mds/pytorch/test_simple.cc)
target_link_libraries(adsl_test_mds_pytorch_simple "${TORCH_LIBRARIES}")
set_property(TARGET adsl_test_mds_pytorch_simple PROPERTY CXX_STANDARD 14)
add_executable(adsl_test_mds_pytorch_simple_global mds/pytorch/test_simple.cc)
target_link_libraries(adsl_test_mds_pytorch_simple_global "${TORCH_LIBRARIES}" global mds)
set_property(TARGET adsl_test_mds_pytorch_simple_global PROPERTY CXX_STANDARD 14)
add_executable(adsl_test_mds_pytorch_basic mds/pytorch/test_basic.cc)
target_link_libraries(adsl_test_mds_pytorch_basic global mds)
set_property(TARGET adsl_test_mds_pytorch_basic PROPERTY CXX_STANDARD 14)

add_executable(adsl_test_mds_pytorch_predictor mds/pytorch/predictor.cc)
target_link_libraries(adsl_test_mds_pytorch_predictor global mds common)
#set_property(TARGET adsl_test_mds_pytorch_predictor PROPERTY CXX_STANDARD 14)

add_executable(adsl_test_mds_pytorch_verify mds/pytorch/verify.cc)
target_link_libraries(adsl_test_mds_pytorch_verify global mds common)

add_executable(adsl_test_mds_pytorch_origin mds/pytorch/origin.cc)
target_link_libraries(adsl_test_mds_pytorch_origin "${TORCH_LIBRARIES}" global)
set_property(TARGET adsl_test_mds_pytorch_origin PROPERTY CXX_STANDARD 14)

add_executable(test_thread test.cpp)
target_link_libraries(test_thread common)

add_executable(adsl_test_mutex common/test_mutex.cc)
target_link_libraries(adsl_test_mutex common)

add_executable(adsl_test_cond_wait common/test_cond_wait.cc)
target_link_libraries(adsl_test_cond_wait common)
