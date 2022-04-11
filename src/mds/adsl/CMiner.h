// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _CMINER_H_

#define _CMINER_H_

#include "OnlineMiner.h"
#include "common/Thread.h"
#include "common/Mutex.h"
#include "common/Cond.h"

class CMiner : public OnlineMiner, public Thread {
  public:
    CMiner(MDBalancer * balancer, int am_supp, float am_conf, int m_epoch, int win_size, int m_gap, int m_supp, float m_conf);

    ~CMiner();

    // used for thread of mining
    Mutex mine_lock;
    Cond mine_cond;
  
    void hit(inodeno_t ino) override;
    
    map<inodeno_t, pair<int, float> >& get_correlated(inodeno_t ino) override;
    map<inodeno_t, map<inodeno_t, pair<int, float> > >& get_full_correlated_table() override;
    
  protected:
    void process() override;

  private:
    // 用于correlation_table if valid
    int overall_min_support;
    float overall_min_confidence;

    // 用于correlation(mine) if valid
    int window_size;
    int max_gap;
    int min_support;
    float min_confidence;

    int max_epoch;

    vector<inodeno_t> input;
    mutex input_lock;

    vector<map<inodeno_t, map<inodeno_t, pair<int, float> > > > correlation_series;
    map<inodeno_t, map<inodeno_t, pair<int, float> > > correlation_table;

    void from_series_to_table();

    void *entry() override; // override the thread entry in Thread

    void signal_miner(); // use to signal the miner thread

    int last_epoch;
};

#endif // _CMINER_H_
