 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sstream>

#include "MDSMonitor.h"

#include "common/debug.h"
#include "mds/MDSRank.h"
#include "mds/Server.h"
#include "mds/MDBalancer.h"

#include "adsl/dout_wrapper.h"

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
}

void MDSMonitor::record_switch_epoch(int beat_epoch, utime_t now)
{
  dout(g_conf->adsl_mds_mon_debug_level_creq_mig_contention)
    << "ADSL_MDS_MON_DEBUG_CREQ_MIG_CONTENTION P "
    << std::fixed << double(now)
    << dendl;
}

/*
void MDSMonitor::record_migration(CDir * dir, utime_t start, utime_t end, bool is_export, bool is_cancelled)
{
  dout(g_conf->adsl_mds_mon_debug_level_creq_mig_contention)
    << "ADSL_MDS_MON_DEBUG_CREQ_MIG_CONTENTION "
    << (is_export ? "E " : "I ") << (is_cancelled ? "C " : "S ")
    << std::fixed << double(start) << ' ' << double(end - start) << ' '
    //<< dout_wrapper<CDir*>(dir) << dendl;
    << dir->dirfrag() << ' ' << dir->get_path() << dendl;
}
*/

void MDSMonitor::record_export(CDir * dir, export_timestamp_trace &export_trace, utime_t finalize)
{
  dout(g_conf->adsl_mds_mon_debug_level_creq_mig_contention)
    << "ADSL_DEBUG_MIGRATION E " << export_trace.final_state
    << std::fixed
    << double(export_trace.lock_start) << ' '
    << double(export_trace.discover_start) << ' '
    << double(export_trace.freeze_start) << ' '
    << double(export_trace.prep_start) << ' '
    << double(export_trace.warn_start) << ' '
    << double(export_trace.export_start) << ' '
    << double(export_trace.loggingfinish_start) << ' '
    << double(export_trace.notify_start) << ' '
    << finalize << ' '
    << dir->dirfrag() << ' ' << dir->get_path() << dendl;
}

void MDSMonitor::record_import(CDir * dir, import_timestamp_trace &import_trace, utime_t finalize)
{
  dout(g_conf->adsl_mds_mon_debug_level_creq_mig_contention)
    << "ADSL_DEBUG_MIGRATION I " << import_trace.final_state
    << std::fixed
    << double(import_trace.discovering_start) << ' '
    << double(import_trace.discovered_start) << ' '
    << double(import_trace.prepping_start) << ' '
    << double(import_trace.prepped_start) << ' '
    << double(import_trace.loggingstart_start) << ' '
    << double(import_trace.ack_start) << ' '
    << double(import_trace.finish_start) << ' '
    << double(import_trace.loggingfinish_start) << ' '
    << double(import_trace.notify_start) << ' '
    << finalize << ' '
    << dir->dirfrag() << ' ' << dir->get_path() << dendl;
}

void MDSMonitor::record_client_request(MDRequestRef & mdr, utime_t end)
{
  MClientRequest * creq = mdr->client_request;
  utime_t start = creq->get_dispatch_stamp();
  dout(g_conf->adsl_mds_mon_debug_level_creq_mig_contention)
    << "ADSL_MDS_MON_DEBUG_CREQ_MIG_CONTENTION C "
    << std::fixed << double(start) << ' ' << double(end - start) << ' '
    << mdr->retry << ' '
    //<< *creq << dendl;
    << creq->get_tid() << ' ' << ceph_mds_op_name(creq->get_op()) << ' '
    << creq->get_filepath() << dendl;
}

}; /* namespace: adsl */
