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

#ifndef ADSL_CEPH_MEXPORTDIRNONBLOCKACK_H
#define ADSL_CEPH_MEXPORTDIRNONBLOCKACK_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirNonBlockAck : public Message {
public:
  // MExportDirDiscoverAck / MExportDirPrepAck
  dirfrag_t dirfrag;
  bool success;

  // MExportDirAck
  bufferlist imported_caps;

public:
  inodeno_t get_ino() { return dirfrag.ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  bool is_success() { return success; }

  MExportDirNonBlockAck() : Message(MSG_MDS_EXPORTDIR_NONBLOCK_ACK) {}
  MExportDirNonBlockAck(dirfrag_t df, uint64_t tid, bool s=true) :
    Message(MSG_MDS_EXPORTDIR_NONBLOCK_ACK),
    dirfrag(df), success(s) {
    set_tid(tid);
  }
private:
  ~MExportDirNonBlockAck() override {}

public:
  const char *get_type_name() const override { return "NBExA"; }
  void print(ostream& o) const override {
    o << "export_nonblock_ack(" << dirfrag;
    if (success) 
      o << " success)";
    else
      o << " failure)";
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(success, p);
    ::decode(imported_caps, p);
  }
  void encode_payload(uint64_t features) override {
    ::encode(dirfrag, payload);
    ::encode(success, payload);
    ::encode(imported_caps, payload);
  }
};

#endif
