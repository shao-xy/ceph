// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "mds/mdstypes.h"

namespace adsl {

dirfrag_load_t::dirfrag_load_t(const utime_t &now, CDir * dir, MDBalancer * bal, string name)
    : decay_load(now), pred_load(dir, bal, this), name(name) {}

void dirfrag_load_t::dump(Formatter *f) const
{
  f->open_object_section("dirfrag_load_vec_t");
  decay_load.dump(f);
  f->close_section();
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
