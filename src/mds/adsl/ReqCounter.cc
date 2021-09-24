// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "ReqCounter.h"

ReqCounter::ReqCounter(int queue_len)
  : _data(queue_len)
{
}

void ReqCounter::switch_epoch(int epoch_num)
{
  std::unique_lock l(mut);

  if (epoch_num <= 0)	return;

  if (epoch_num >= REQCOUNTER_QUEUE_LEN_DEFAULT) {
    _data = list<bool>(REQCOUNTER_QUEUE_LEN_DEFAULT);
    _last_hit = false;
    _cache_hit_times = 0;
    return;
  }
  _cache_hit_times += (int) _last_hit - (int) _data.front();
  _data.pop_front();
  _data.push_back(_last_hit);
  _last_hit = false;
  epoch_num--;
  for (int i = 0; i < epoch_num - 1; i++) {
    _cache_hit_times -= (int) _data.front();
    _data.pop_front();
    _data.push_back(false);
  }
}

// Returns:
//   -1 -> already hit this epoch
//    0 -> first hit this epoch, but ever hit before
//    1 -> brand new hit
int ReqCounter::hit()
{
  std::unique_lock l(mut);
  if (_last_hit) {
    return -1;
  }
  _last_hit = true;
  return !_cache_hit_times;
}
