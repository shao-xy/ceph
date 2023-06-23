 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sstream>

#include "MDSMonitor.h"

#include "common/debug.h"
#include "mds/MDSRank.h"
#include "mds/Server.h"
#include "mds/MDBalancer.h"

#include "common/Thread.h"
class TestThread : public Thread {
  Mutex mut;
public:
  TestThread() : mut("test") {}
protected:
  void * entry() override { return NULL; }
} t;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "ADSL.mds." << mds->get_nodeid() << ".mon "

namespace adsl {

FactorTracer::FactorTracer(MDSRank * mds, bool use_server, int factoridx)
  : DeltaTracerWatcher<int, int>(factoridx), mds(mds), use_server(use_server)
{
  init_now();
} 

int FactorTracer::check_now(int idx) {
  if (use_server)
    return mds->server->logger ? mds->server->logger->get(idx) : 0;
  else
    return mds->logger ? mds->logger->get(idx) : 0;
}

MDSMonitor::MDSMonitor(MDSRank * mds)
    : mds(mds), m_runFlag(true),
    iops_tracer(mds, false, l_mds_request),
    clientreq_tracer(mds, true, l_mdss_handle_client_request),
    slavereq_tracer(mds, true, l_mdss_handle_slave_request),
    fwps_tracer(mds, false, l_mds_forward)
{
  // We start immediately in constructor function
  dout(0) << "Launching monitor thread " << dendl;
  create("ADSL-MDSMonitor");
  dout(0) << "Launched monitor thread " << dendl;
}

MDSMonitor::~MDSMonitor()
{
  m_runFlag = false;
  if (am_self()) {
    // suicide?
    // It's not safe to destruct an object with class derived from Thread class
  }
  else {
    // join();
    kill(SIGTERM);
  }
}

mds_load_t MDSMonitor::mds_load()
{
  return mds->balancer->get_load(ceph_clock_now());
}

void * MDSMonitor::entry()
{
  dout(0) << "MDS_MONITOR thread start." << dendl;
  while (m_runFlag) {
    sleep(1);
    update_and_writelog();
  }
  dout(0) << "MDS_MONITOR thread stop." << dendl;
  return NULL;
}

int MDSMonitor::terminate()
{
  m_runFlag = false;
  if (!am_self())
    return this->join();
  else
    return 0;
}

// to compat with old scripts
#undef dout_prefix
#define dout_prefix *_dout << "ADSL.mds." << mds->get_nodeid() << ".mon monitor_run "
void MDSMonitor::update_and_writelog()
{
  mds_load_t load(mds_load());
  std::stringstream ss;
  ss << "IOPS " << iops_tracer.get(true) // IOPS
    << " IOPS-CLIENT-REQ " << clientreq_tracer.get(true) // IOPS-CLIENT-REQ
    << " IOPS-SLAVE-REQ " << clientreq_tracer.get(true) // IOPS-SLAVE-REQ
    << " Cache-Inodes " << mds->mdcache->lru.lru_get_size() // Cached inodes size
    << " Cache-Inodes-Pinned " << mds->mdcache->lru.lru_get_num_pinned() // Cached inodes pinned
    << " FWPS " << fwps_tracer.get(true) // FWPS
    << " MDSLoad " << load;
  CInode * root = mds->mdcache->get_root();
  if (root) {
    list<CDir*> ls;
    root->get_dirfrags(ls);
    ss << " #DIRFRAG " << ls.size();
  } else {
    ss << " #DIRFRAG 0";
  }
  dout(g_conf->adsl_mds_mon_debug_level) << ss.str() << dendl;

  if (g_conf->adsl_mds_predictor_trace_predict_lat) {
    static int last_epoch = -1;
    if (last_epoch != mds->balancer->get_beat_epoch()) {
      last_epoch = mds->balancer->get_beat_epoch();

      // Warning: no thread protect!
      std::stringstream ss_d;
      for (double d : mds->balancer->predictor_lat_tracer) {
	ss_d << ' ' << d;
      }
      mds->balancer->predictor_lat_tracer.clear();
      dout(0) << " TRACE_PREDICT_LAT" << ss_d.str() << dendl;
    }
  }
}

}; /* namespace: adsl */
