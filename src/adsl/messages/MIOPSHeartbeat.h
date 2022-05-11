// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */


#ifndef SXY_MIOPSHEARTBEAT_H
#define SXY_MIOPSHEARTBEAT_H

#include "include/types.h"
#include "msg/Message.h"

class MIOPSHeartbeat : public Message {
  int load;
  __s32        beat;
  map<mds_rank_t, float> import_map;

 public:
  int get_load() { return load; }
  int get_beat() { return beat; }

  map<mds_rank_t, float>& get_import_map() {
    return import_map;
  }

  MIOPSHeartbeat()
    : Message(MSG_MDS_HEARTBEAT_IOPS), load(utime_t()) { }
  MIOPSHeartbeat(int load, int beat)
    : Message(MSG_MDS_HEARTBEAT_IOPS),
      load(load) {
    this->beat = beat;
  }
private:
  ~MIOPSHeartbeat() override {}

public:
  const char *get_type_name() const override { return "HB_IOPS"; }

  void encode_payload(uint64_t features) override {
    ::encode(load, payload);
    ::encode(beat, payload);
    ::encode(import_map, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(load, p);
    ::decode(beat, p);
    ::decode(import_map, p);
  }

};

#endif
