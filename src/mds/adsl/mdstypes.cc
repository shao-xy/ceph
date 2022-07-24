// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "mds/mdstypes.h"

namespace adsl {

dirfrag_load_t::dirfrag_load_t(const utime_t &now, CDir * dir, MDBalancer * bal, string name)
    : decay_load(now), pred_load(dir, bal), name(name) {}

void dirfrag_load_t::dump(Formatter *f) const
{
  f->open_object_section("dirfrag_load_vec_t");
  decay_load.dump(f);
  f->close_section();
}

dirfrag_load_pred_t::dirfrag_load_pred_t(const dirfrag_load_pred_t & another)
  : dir(another.dir),
  bal(another.bal), mut("dirfrag_load_pred_t"),
  predicted_load(another.predicted_load),
  predicted_epoch(another.predicted_epoch),
  tried_predict_epoch(another.tried_predict_epoch),
  cur_load(another.cur_load),
  cur_epoch(another.cur_epoch),
  from_parent(another.from_parent)
{ }

void dirfrag_load_pred_t::operator=(const dirfrag_load_pred_t & another)
{
  this->dir = another.dir;
  this->bal = another.bal;
  this->predicted_load = another.predicted_load;
  this->predicted_epoch = another.predicted_epoch;
  this->tried_predict_epoch = another.tried_predict_epoch;
  this->cur_load = another.cur_load;
  this->cur_epoch = another.cur_epoch;
  this->from_parent = another.from_parent;
}

void dirfrag_load_pred_t::encode(bufferlist &bl) const {
  ::encode(predicted_load, bl);
  ::encode(predicted_epoch, bl);
  ::encode(cur_load, bl);
  ::encode(cur_epoch, bl);
}
void dirfrag_load_pred_t::decode(bufferlist::iterator &p) {
  ::decode(predicted_load, p);
  ::decode(predicted_epoch, p);
  ::decode(cur_load, p);
  ::decode(cur_epoch, p);
}

};
