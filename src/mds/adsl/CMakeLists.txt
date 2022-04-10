find_package (Python3 3.9 REQUIRED COMPONENTS Interpreter Development)
message(STATUS "Python3_FOUND: ${Python3_FOUND}")
message(STATUS "Python3_VERSION: ${Python3_VERSION}")
message(STATUS "Python3_Development_FOUND: ${Python3_Development_FOUND}")
message(STATUS "Python3_LIBRARIES: ${Python3_LIBRARIES}")
message(STATUS "Python3_INCLUDE_DIRS: ${Python3_INCLUDE_DIRS}")

add_library(mds-adsl-py STATIC
  PyPredictor.cc
)
target_include_directories(mds-adsl-py PRIVATE "${Python3_INCLUDE_DIRS}")
target_link_libraries(mds-adsl-py ${Python3_LIBRARIES})

add_library(mds-adsl STATIC
  MDSMonitor.cc
  Predictor.cc
  LuaPredictor.cc
  SocketPredictor.cc
  TorchPredictor.cc
  MatrixFitter.cc
  CMinerPredictor.cc
  CMiner.cc
)
target_include_directories(mds-adsl PRIVATE "${Python3_INCLUDE_DIRS}")
target_link_libraries(mds-adsl liblua common "${TORCH_LIBRARIES}" adsl)
set_property(TARGET mds-adsl PROPERTY CXX_STANDARD 14)
