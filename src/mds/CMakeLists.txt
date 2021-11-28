find_package (Python3 3.9 REQUIRED COMPONENTS Interpreter Development)
message("Python3_FOUND: ${Python3_FOUND}")
message("Python3_VERSION: ${Python3_VERSION}")
message("Python3_Development_FOUND: ${Python3_Development_FOUND}")
message("Python3_LIBRARIES: ${Python3_LIBRARIES}")
message("Python3_INCLUDE_DIRS: ${Python3_INCLUDE_DIRS}")
set(mds_srcs
  Capability.cc
  MDSDaemon.cc
  MDSRank.cc
  Beacon.cc
  flock.cc
  locks.c
  journal.cc
  Server.cc
  Mutation.cc
  MDCache.cc
  RecoveryQueue.cc
  StrayManager.cc
  PurgeQueue.cc
  Locker.cc
  Migrator.cc
  MDBalancer.cc
  CDentry.cc
  CDir.cc
  CInode.cc
  LogEvent.cc
  MDSTable.cc
  InoTable.cc
  JournalPointer.cc
  MDSTableClient.cc
  MDSTableServer.cc
  ScrubStack.cc
  DamageTable.cc
  SimpleLock.cc
  SnapRealm.cc
  SnapServer.cc
  snap.cc
  SessionMap.cc
  MDSContext.cc
  MDSAuthCaps.cc
  MDLog.cc
  MDSCacheObject.cc
  Mantle.cc
  adsl/MDSMonitor.cc
  adsl/Predictor.cc
  adsl/LuaPredictor.cc
  adsl/PyPredictor.cc
  adsl/SocketPredictor.cc
  ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
  ${CMAKE_SOURCE_DIR}/src/osdc/Journaler.cc)
add_library(mds STATIC ${mds_srcs}
  $<TARGET_OBJECTS:heap_profiler_objs>)
target_include_directories(mds PRIVATE "${Python3_INCLUDE_DIRS}")
target_link_libraries(mds ${ALLOC_LIBS} osdc liblua ${Python3_LIBRARIES})
#target_link_libraries(mds ${ALLOC_LIBS} osdc liblua ${Python3_LIBRARIES} Boost::python ${PYTHON_LIBRARIES})
