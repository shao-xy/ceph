// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef ADSL_CEPH_MEXPORTDIRNONBLOCK_H
#define ADSL_CEPH_MEXPORTDIRNONBLOCK_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirNonBlock : public Message {
  // MExportDirDiscover
  mds_rank_t from;
  dirfrag_t dirfrag;
  filepath path;

public:
  bool started;

  // MExportDirPrep
public:
  bufferlist basedir;
  list<dirfrag_t> bounds;
  list<bufferlist> traces;
private:
  set<mds_rank_t> bystanders;
  bool b_did_assim;

  // MExportDir
public:
  bufferlist export_data;
  vector<dirfrag_t> e_bounds;
  bufferlist client_map;

  // MExportDirDiscover
public:
  mds_rank_t get_source_mds() { return from; }
  inodeno_t get_ino() { return dirfrag.ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  filepath& get_path() { return path; }

  // MExportDirPrep
  list<dirfrag_t>& get_bounds() { return bounds; }
  set<mds_rank_t> &get_bystanders() { return bystanders; }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirNonBlock() :
    Message(MSG_MDS_EXPORTDIR_NONBLOCK),
    started(false) {
    b_did_assim = false;
  }
  MExportDirNonBlock(
      dirfrag_t df, filepath& p, mds_rank_t f, uint64_t tid
      ) :
    Message(MSG_MDS_EXPORTDIR_NONBLOCK),
    from(f), dirfrag(df), path(p), started(false),
    b_did_assim(false)
  {
    set_tid(tid);
  }
private:
  ~MExportDirNonBlock() override {}

public:
  const char *get_type_name() const override { return "NBEx"; }
  void print(ostream& o) const override {
    o << "export_nonblock("
      << dirfrag << " " << path
      << ")";
  }

  // MExportDirPrep
  void add_bound(dirfrag_t df) {
    bounds.push_back( df );
  }
  void add_trace(bufferlist& bl) {
    traces.push_back(bl);
  }
  void add_bystander(mds_rank_t who) {
    bystanders.insert(who);
  }

  // MExportDir
  void add_export(dirfrag_t df) { 
    e_bounds.push_back(df); 
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    // MExportDirDiscover
    ::decode(from, p);
    ::decode(dirfrag, p);
    ::decode(path, p);
    // MExportDirPrep
    ::decode(basedir, p);
    ::decode(bounds, p);
    ::decode(traces, p);
    ::decode(bystanders, p);
    // MExportDir
    ::decode(e_bounds, p);
    ::decode(export_data, p);
    ::decode(client_map, p);
  }

  void encode_payload(uint64_t features) override {
    // MExportDirDiscover
    ::encode(from, payload);
    ::encode(dirfrag, payload);
    ::encode(path, payload);
    // MExportDirPrep
    ::encode(basedir, payload);
    ::encode(bounds, payload);
    ::encode(traces, payload);
    ::encode(bystanders, payload);
    // MExportDir
    ::encode(e_bounds, payload);
    ::encode(export_data, payload);
    ::encode(client_map, payload);
  }
};

#endif 
