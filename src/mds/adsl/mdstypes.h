// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef _ADSL_MDS_MDSTYPES_H_
#define _ADSL_MDS_MDSTYPES_H_

// NEVER include this file directly
// here are some lines to be injected into mds/mdstypes.h

#define ADSL_METADATA_SYS "M4"

#include "mds/mdstypes.h"

class dirfrag_load_vec_t;

namespace adsl {

class dirfrag_load_pred_t {
public:
  double _load;

  dirfrag_load_pred_t() {}
  void encode(bufferlist &bl) const {
  }
  void decode(bufferlist::iterator &p) {
  }
  void adjust(double d) {
  }
  double meta_load() {
  }
};

class dirfrag_load_t {
public:
  dirfrag_load_vec_t decay_load;
  dirfrag_load_pred_t pred_load;
  bool use_pred;

  dirfrag_load_t() {}
  explicit dirfrag_load_t(const utime_t &now)
    : decay_load(now) {}

  void encode(bufferlist &bl) const {
  }
  void decode(bufferlist::iterator &p) {
  }
  void adjust(utime_t now, const DecayRate& rate, double d) {
  }
  void zero(utime_t now) {
  }
  double meta_load(utime_t now, const DecayRate& rate) {
    return use_pred ? pred_load.meta_load() : decay_load.meta_load(now, rate);
  }
  double meta_load() {
    return use_pred ? pred_load.meta_load() : decay_load.meta_load();
  }

  void add(utime_t now, DecayRate& rate, dirfrag_load_vec_t& r) {
  }
  void sub(utime_t now, DecayRate& rate, dirfrag_load_vec_t& r) {
  }
  void scale(double f) {
  }
};

};

#endif /* mds/adsl/mdstypes.h */
