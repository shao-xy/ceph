// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDSTYPES_H
#pragma message("Fatal: do not include this file directly.")
#error "mds/mdstypes.h not included"
#endif

#ifndef _ADSL_MDS_MDSTYPES_H_
#define _ADSL_MDS_MDSTYPES_H_

#include <vector>
using std::vector;
#include <algorithm>
#include <numeric>

#include "include/encoding.h"
#include "common/Mutex.h"

// NEVER include this file directly
// here are some lines to be injected into mds/mdstypes.h

#define ADSL_METADATA_SYS "MOIST"

//#define RECENT_LOAD_EPOCH_LENGTH 10
#define RECENT_LOAD_EPOCH_LENGTH 48

class CDir;
class CInode;
class MDBalancer;
namespace adsl {

class Predictor;

template <typename T>
struct LoadArray {
  vector<T> nums;

  LoadArray() {}
  LoadArray(int len)
    : nums(len) {}
  LoadArray(T nums[], int len)
    : nums(nums, nums + len) {}
  LoadArray(vector<T>& nums)
    : nums(nums) {}
  LoadArray(vector<T>&& nums)
    : nums(nums) {}
  T total() {
    return std::accumulate(nums.begin(), nums.end(), static_cast<T>(0));
  }
  void shift(size_t n = 1, T next = static_cast<T>(0)) {
    if (n <= 0)	return;

    size_t len = nums.size();
    if (n >= len) {
      nums = vector<T>(len);
      return;
    }

    for (size_t i = 0; i < len - n; i++) {
      nums[i] = nums[i + n];
    }
    nums[len - n] = next;
    for (size_t i = len - n + 1; i < len; i++) {
      nums[i] = 0;
    }
  }
  size_t size() { return nums.size(); }
  void clear() { nums.clear(); }
  void append(T v) { nums.push_back(v); }
  typename vector<T>::iterator begin() { return nums.begin(); }
  typename vector<T>::iterator end() { return nums.end(); }
  T& operator[](size_t i) { return nums[i]; }
  T* data() { return nums.data(); }
  const T* data() const { return nums.data(); }

  void encode(bufferlist &bl) const {
    ::encode(nums, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode(nums, p);
  }
};

template <typename T>
std::ostream & operator<<(std::ostream & os, LoadArray<T> & la)
{
  os << "[ ";
  for (auto it = la.begin();
       it != la.end();
       it++) {
    os << *it << ' ';
  }
  return os << ']';
}

using LoadArray_Int = LoadArray<int>;
using LoadArray_Double = LoadArray<double>;

};

template <typename T>
inline void encode(const adsl::LoadArray<T> &c, bufferlist &bl) {
  c.encode(bl);
}

template <typename T>
inline void decode(adsl::LoadArray<T> &c, bufferlist::iterator &p) {
  c.decode(p);
}


namespace adsl {

class dirfrag_load_t;

class dirfrag_load_pred_t {
  CDir * dir;
  MDBalancer * bal;
  map<int, CInode*> pos_map;
  vector<LoadArray_Int> load_matrix;

  Mutex mut;

  // this functions is defined in MDBalancer.cc
  vector<LoadArray_Int> load_prepare();
  inline bool use_parent_fast();
public:
  double predicted_load;
  int predicted_epoch;
  int tried_predict_epoch;
  double cur_load;
  int cur_epoch;
  bool from_parent;

  dirfrag_load_pred_t() : dirfrag_load_pred_t(NULL, NULL) {}
  explicit dirfrag_load_pred_t(CDir * dir, MDBalancer * bal)
    : dir(dir), bal(bal), mut("dirfrag_load_pred_t"),
      predicted_load(0.0), predicted_epoch(-1), tried_predict_epoch(-1),
      cur_load(0.0), cur_epoch(-1), from_parent(false) {}
  dirfrag_load_pred_t(const dirfrag_load_pred_t & another);
  void operator=(const dirfrag_load_pred_t & another);

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void adjust(double d) {
    Mutex::Locker l(mut);
    force_current_epoch();
    cur_load += d;
  }
  void zero() {
    Mutex::Locker l(mut);
    force_current_epoch();
    cur_load = 0;
  }


  // these functions are defined in MDBalancer.cc
  // These function are protected by async locks. Each dirfrag_load_pred_t struct might be modified within only one thread.
private:
  void force_current_epoch(int epoch = -1);
  int do_predict(Predictor * predictor);

public:
  inline bool should_use();
  double meta_load(Predictor * predictor = NULL);

  void add(dirfrag_load_pred_t& r) {
    Mutex::Locker l(mut);
    force_current_epoch();
    r.force_current_epoch();
    cur_load += r.cur_load;
  }
  void sub(dirfrag_load_pred_t& r) {
    Mutex::Locker l(mut);
    force_current_epoch();
    r.force_current_epoch();
    cur_load -= r.cur_load;
  }
  void scale(double f) {
    Mutex::Locker l(mut);
    force_current_epoch();
    cur_load *= f;
  }
  CDir * get_dir() { return dir; }
  MDBalancer * get_balancer() { return bal; }
  inline void set_balancer(MDBalancer * bal) { this->bal = bal; }
  std::ostream& print(std::ostream& out);
};
inline std::ostream& operator<<(std::ostream& out, dirfrag_load_pred_t& dlp)
{
  return dlp.print(out);
}

};

WRITE_CLASS_ENCODER(adsl::dirfrag_load_pred_t)

namespace adsl {
class dirfrag_load_t {
public:
  dirfrag_load_vec_t decay_load;
  dirfrag_load_pred_t pred_load;
  string name; // this field is left as empty string after (de)serialization

  dirfrag_load_t() {}
  explicit dirfrag_load_t(const utime_t &now, CDir * dir, MDBalancer * bal, string name = "");

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(decay_load, bl);
    ::encode(pred_load, bl);
    ENCODE_FINISH(bl);
  }
  void decode(const utime_t& now, bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(decay_load, now, bl);
    ::decode(pred_load, bl);
    DECODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    utime_t sample;
    decode(sample, bl);
  }
  void dump(Formatter *f) const;
  DecayCounter &get(int t) {
    return decay_load.get(t);
  }
  void adjust(utime_t now, const DecayRate& rate, double d) {
    decay_load.adjust(now, rate, d);
    pred_load.adjust(d);
  }
  void zero(utime_t now) {
    decay_load.zero(now);
    pred_load.zero();
  }

  // these two functions defined in MDBalancer.cc
  double meta_load(utime_t now, const DecayRate& rate);
  double meta_load(Predictor * predictor = NULL);

  void add(utime_t now, DecayRate& rate, dirfrag_load_t& r) {
    decay_load.add(now, rate, r.decay_load);
    pred_load.add(r.pred_load);
  }
  void sub(utime_t now, DecayRate& rate, dirfrag_load_t& r) {
    decay_load.sub(now, rate, r.decay_load);
    pred_load.sub(r.pred_load);
  }
  void scale(double f) {
    decay_load.scale(f);
    pred_load.scale(f);
  }
  inline void set_balancer(MDBalancer * bal) { pred_load.set_balancer(bal); }
};
inline std::ostream& operator<<(std::ostream& out, dirfrag_load_t& dl)
{
  return out << "{" << dl.decay_load << ", " << dl.pred_load << "}";
}

};

inline void encode(const adsl::dirfrag_load_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(adsl::dirfrag_load_t &c, const utime_t &t, bufferlist::iterator &p) {
  c.decode(t, p);
}
inline void decode(adsl::dirfrag_load_t &c, bufferlist::iterator &p) {
  utime_t sample;
  c.decode(sample, p);
}

#endif /* mds/adsl/mdstypes.h */
