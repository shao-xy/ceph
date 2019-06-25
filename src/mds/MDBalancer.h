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
#ifndef CEPH_MDBALANCER_H
#define CEPH_MDBALANCER_H

#include "include/types.h"
#include "common/Clock.h"
#include "common/Cond.h"
#include "mds/mdstypes.h"

#include "msg/Message.h"
#include "messages/MHeartbeat.h"

#include "MDSMap.h"

#include "mds/adsl/ReqTracer.h"

class MDSRank;
class MHeartbeat;
class MIFBeat;
class CInode;
class CDir;
class Messenger;
class MonClient;

class MDBalancer {
  friend class C_Bal_SendHeartbeat;
  friend class C_Bal_SendIFbeat;
public:
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;
  friend class C_Bal_SendHeartbeat;

  MDBalancer(MDSRank *m, Messenger *msgr, MonClient *monc);

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  int proc_message(const cref_t<Message> &m);

  /**
   * Regularly called upkeep function.
   *
   * Sends MHeartbeat messages to the mons.
   */
  void tick();

  void handle_export_pins(void);

  void subtract_export(CDir *ex);
  void add_import(CDir *im);
  void adjust_pop_for_rename(CDir *pdir, CDir *dir, bool inc);

  void hit_inode(CInode *in, int type, int who=-1);
  void hit_dir(CDir *dir, int type, int who=-1, double amount=1.0, int newold=-2);
  void update_dir_pot_recur(CDir * dir, int level, double adj_auth_pot = 1.0, double adj_all_pot = 1.0);

  void queue_split(const CDir *dir, bool fast);
  void queue_merge(CDir *dir);

  /**
   * Based on size and configuration, decide whether to issue a queue_split
   * or queue_merge for this CDir.
   *
   * \param hot whether the directory's temperature is enough to split it
   */
  void maybe_fragment(CDir *dir, bool hot);
  
  // try to dynamically split dir
  void dynamically_fragment(CDir *dir, double amount);

  void handle_mds_failure(mds_rank_t who);

  int dump_loads(Formatter *f) const;

private:
  typedef struct {
    std::map<mds_rank_t, double> targets;
    std::map<mds_rank_t, double> imported;
    std::map<mds_rank_t, double> exported;
  } balance_state_t;


  typedef struct{
    double my_if;
    double my_urgency;
    mds_rank_t whoami;
    bool is_bigger;
  }imbalance_summary_t;
  static bool sortImporter (imbalance_summary_t i,imbalance_summary_t j) { return (i.my_if > j.my_if); };

  //set up the rebalancing targets for export and do one if the
  //MDSMap is up to date
  void prep_rebalance(int beat);
  int mantle_prep_rebalance();

  mds_load_t get_load();
  int localize_balancer();
  void send_heartbeat();
  void send_ifbeat(mds_rank_t target, double if_beate_value, vector<migration_decision_t>& migration_decision);
  void handle_heartbeat(const cref_t<MHeartbeat> &m);
  void find_exports_coldfirst_trigger(CDir *dir,
                              double amount,
                              list<CDir*>& exports,
                              double& have, mds_rank_t dest,
                              set<CDir*>& already_exporting);
  void find_exports_coldfirst(CDir *dir,
                    double amount,
                    list<CDir*>& exports,
                    double& have,
                    set<CDir*>& already_exporting,
                    mds_rank_t dest,
                    int descend_depth);
  //void export_empties();
  void handle_ifbeat(MIFBeat *m);
  void simple_determine_rebalance(vector<migration_decision_t>& migration_decision);
  void find_exports(CDir *dir,
                    double amount,
                    std::vector<CDir*>* exports,
                    double& have,
                    set<CDir*>& already_exporting,
		    mds_rank_t target=0);

  void find_exports_wrapper(CDir *dir,
                    double amount,
                    list<CDir*>& exports,
                    double& have,
                    set<CDir*>& already_exporting,
		    mds_rank_t target);

  double try_match(balance_state_t &state,
                   mds_rank_t ex, double& maxex,
                   mds_rank_t im, double& maxim);

  double get_maxim(balance_state_t &state, mds_rank_t im) {
    return target_load - mds_meta_load[im] - state.imported[im];
  }
  double get_maxex(balance_state_t &state, mds_rank_t ex) {
    return mds_meta_load[ex] - target_load - state.exported[ex];
  }

  /**
   * Try to rebalance.
   *
   * Check if the monitor has recorded the current export targets;
   * if it has then do the actual export. Otherwise send off our
   * export targets message again.
   */
  void try_rebalance(balance_state_t& state);

  bool bal_fragment_dirs;
  int64_t bal_fragment_interval;
  static const unsigned int AUTH_TREES_THRESHOLD = 5;

  MDSRank *mds;
  Messenger *messenger;
  MonClient *mon_client;
  int beat_epoch = 0;

  string bal_code;
  string bal_version;

  time last_heartbeat = clock::zero();
  time last_sample = clock::zero();
  time rebalance_time = clock::zero(); //ensure a consistent view of load for rebalance

  time last_get_load = clock::zero();
  uint64_t last_num_requests = 0;
  uint64_t last_cpu_time = 0;

  // Dirfrags which are marked to be passed on to MDCache::[split|merge]_dir
  // just as soon as a delayed context comes back and triggers it.
  // These sets just prevent us from spawning extra timer contexts for
  // dirfrags that already have one in flight.
  set<dirfrag_t> split_pending, merge_pending;

  // per-epoch scatter/gathered info
  std::map<mds_rank_t, mds_load_t> mds_load;
  std::map<mds_rank_t, double> mds_meta_load;
  std::map<mds_rank_t, map<mds_rank_t, float> > mds_import_map;
  std::map<mds_rank_t, int> mds_last_epoch_under_map;
  //map<mds_rank_t, float> old_req;

  // per-epoch state
  double my_load = 0;
  double target_load = 0;

  friend class CDir;
  friend class mds_load_t;
  ReqTracer req_tracer;
public:
  double calc_mds_load(mds_load_t load, bool auth = false);
};
#endif
