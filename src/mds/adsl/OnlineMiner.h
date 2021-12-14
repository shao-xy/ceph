// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef __ADSL_ONLINE_MINER_H__
#define __ADSL_ONLINE_MINER_H__

#include <vector>
using std::vector;
#include <map>
using std::map;

#include "include/fs_types.h"

class OnlineMiner {
  public:
    // constructor and (virtual) destructor
    OnlineMiner() {}
    virtual ~OnlineMiner() {}

    // this function is called by MDSBalancer::hit_inode()
    // or MDSBalancer::hit_dir().
    // make sure it NEVER called more than once for each request
    virtual void hit(inodeno_t ino) = 0;
    
    // this two functions are called before prediction
    virtual vector<inodeno_t> get_correlated(inodeno_t ino) = 0;
    virtual map<inodeno_t, vector<inodeno_t>> get_full_correlated_table() = 0;

  protected:
    // this function is invoked by the miner thread
    virtual void process() = 0;
};

#endif
