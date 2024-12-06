 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __MDS_ADSL_MDSMONITOR_H__
#define __MDS_ADSL_MDSMONITOR_H__

#include "common/Thread.h"

#include "mds/mdstypes.h"

#include "messages/MClientRequest.h"

#include "DeltaTracer.h"

class MDSRank;
class MDRequestImpl;
typedef boost::intrusive_ptr<MDRequestImpl> MDRequestRef;

namespace adsl {

class FactorTracer : public DeltaTracerWatcher<int, int> {
    MDSRank * mds;
    bool use_server;

  public:
    FactorTracer(MDSRank * mds, bool use_server, int factoridx);
    int check_now(int idx) override;
};

class MDSMonitor : public Thread {
    MDSRank * mds;
    bool m_runFlag;
  
    FactorTracer iops_tracer;
    FactorTracer clientreq_tracer;
    FactorTracer slavereq_tracer;
    FactorTracer fwps_tracer;

    mds_load_t mds_load();
  
  private:
    void update_and_writelog();
  protected:
    void * entry() override;
  
  public:
    MDSMonitor(MDSRank * mds = NULL);
    ~MDSMonitor();
  
    int terminate();

    void record_switch_epoch(int beat_epoch, utime_t now);
    void record_export(CDir * dir, export_timestamp_trace &export_trace, utime_t finalize = ceph_clock_now());
    void record_import(CDir * dir, import_timestamp_trace &import_trace, utime_t finalize = ceph_clock_now());
    void record_client_request(MDRequestRef & mdr, utime_t end);
};

}; /* namespace: adsl */

#endif /* mds/adsl/MDSMonitor.h */
