// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OnlineMiner.h"

class CMiner : public OnlineMiner {
  public:
    CMiner();
    ~CMiner();

  
    void hit(inodeno_t ino) override;
    
    vector<inodeno_t> get_correlated(inodeno_t ino) override;
    map<inodeno_t, vector<inodeno_t>> get_full_correlated_table() override;
    
  protected:
    void process() override;
};
