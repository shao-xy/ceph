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

#include "include/compat.h"
#include "mdstypes.h"

#include "mon/MonClient.h"
#include "MDBalancer.h"
#include "MDSRank.h"
#include "MDSMap.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"
#include "Migrator.h"
#include "Mantle.h"
#include "Server.h"

#include "include/Context.h"
#include "msg/Messenger.h"
#include "messages/MHeartbeat.h"
#include "messages/MIFBeat.h"

#include <fstream>
#include <iostream>
#include <vector>
#include <map>
#include <functional>

using std::map;
using std::vector;
using std::chrono::duration_cast;

#include "common/config.h"
#include "common/errno.h"

#include "adsl/PathUtil.h"

//#define MDS_MONITOR
#include <unistd.h>
#define MDS_COLDFIRST_BALANCER

#define dout_context g_ceph_context
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".bal " << __func__ << " "
#undef dout
#define dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_balancer, lvl)) {\
      subsys = ceph_subsys_mds_balancer;\
    }\
    dout_impl(dout_context, ceph::dout::need_dynamic(subsys), lvl) dout_prefix
#undef dendl
#define dendl dendl_impl; } while (0)

#define COLDSTART_MIGCOUNT 1000

#define MIN_LOAD    50   //  ??
#define MIN_REEXPORT 5  // will automatically reexport
#define MIN_OFFLOAD 0.1   // point at which i stop trying, close enough

//#define MAX_EXPORT_SIZE 10000

#define LUNULE_DEBUG_LEVEL 7
#define LUNULE_MIG_AMOUNT 0.25

int MDBalancer::proc_message(const cref_t<Message> &m)
{
  switch (m->get_type()) {

  case MSG_MDS_HEARTBEAT:
    handle_heartbeat(ref_cast<MHeartbeat>(m));
    break;
  case MSG_MDS_IFBEAT:
    handle_ifbeat(static_cast<MIFBeat*>(m));
    break;
    
  default:
    derr << " balancer unknown message " << m->get_type() << dendl_impl;
    ceph_abort_msg("balancer unknown message");
  }

  return 0;
}

MDBalancer::MDBalancer(MDSRank *m, Messenger *msgr, MonClient *monc) :
    mds(m), messenger(msgr), mon_client(monc)
{
  bal_fragment_dirs = g_conf().get_val<bool>("mds_bal_fragment_dirs");
  bal_fragment_interval = g_conf().get_val<int64_t>("mds_bal_fragment_interval");
}

void MDBalancer::handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map)
{
  if (changed.count("mds_bal_fragment_dirs"))
    bal_fragment_dirs = g_conf().get_val<bool>("mds_bal_fragment_dirs");
  if (changed.count("mds_bal_fragment_interval"))
    bal_fragment_interval = g_conf().get_val<int64_t>("mds_bal_fragment_interval");
}

void MDBalancer::handle_export_pins(void)
{
  auto &q = mds->mdcache->export_pin_queue;
  auto it = q.begin();
  dout(20) << "export_pin_queue size=" << q.size() << dendl;
  while (it != q.end()) {
    auto cur = it++;
    CInode *in = *cur;
    ceph_assert(in->is_dir());

    in->check_pin_policy();
    mds_rank_t export_pin = in->get_export_pin(false);
    if (export_pin >= mds->mdsmap->get_max_mds()) {
      dout(20) << " delay export_pin=" << export_pin << " on " << *in << dendl;
      in->state_clear(CInode::STATE_QUEUEDEXPORTPIN);
      q.erase(cur);

      in->state_set(CInode::STATE_DELAYEDEXPORTPIN);
      mds->mdcache->export_pin_delayed_queue.insert(in);
      continue;
    } else {
      dout(20) << " executing export_pin=" << export_pin << " on " << *in << dendl;
    }

    bool remove = true;
    for (auto&& dir : in->get_dirfrags()) {
      if (!dir->is_auth())
	continue;

      if (export_pin == MDS_RANK_NONE) {
	if (dir->state_test(CDir::STATE_AUXSUBTREE)) {
	  if (dir->is_frozen() || dir->is_freezing()) {
	    // try again later
	    remove = false;
	    continue;
	  }
	  dout(10) << " clear auxsubtree on " << *dir << dendl;
	  dir->state_clear(CDir::STATE_AUXSUBTREE);
	  mds->mdcache->try_subtree_merge(dir);
	}
      } else if (export_pin == mds->get_nodeid()) {
        if (dir->state_test(CDir::STATE_AUXSUBTREE)) {
          ceph_assert(dir->is_subtree_root());
        } else if (dir->state_test(CDir::STATE_CREATING) ||
	           dir->is_frozen() || dir->is_freezing()) {
	  // try again later
	  remove = false;
	  continue;
	} else if (!dir->is_subtree_root()) {
	  dir->state_set(CDir::STATE_AUXSUBTREE);
	  mds->mdcache->adjust_subtree_auth(dir, mds->get_nodeid());
	  dout(10) << " create aux subtree on " << *dir << dendl;
	} else {
	  dout(10) << " set auxsubtree bit on " << *dir << dendl;
	  dir->state_set(CDir::STATE_AUXSUBTREE);
	}
      } else {
        /* Only export a directory if it's non-empty. An empty directory will
         * be sent back by the importer.
         */
        if (dir->get_num_head_items() > 0) {
	  mds->mdcache->migrator->export_dir(dir, export_pin);
        }
	remove = false;
      }
    }

    if (remove) {
      in->state_clear(CInode::STATE_QUEUEDEXPORTPIN);
      q.erase(cur);
    }
  }

  std::vector<CDir *> authsubs = mds->mdcache->get_auth_subtrees();
  bool print_auth_subtrees = true;

  if (authsubs.size() > AUTH_TREES_THRESHOLD &&
      !g_conf()->subsys.should_gather<ceph_subsys_mds, 25>()) {
    dout(15) << "number of auth trees = " << authsubs.size() << "; not "
		"printing auth trees" << dendl;
    print_auth_subtrees = false;
  }

  for (auto &cd : authsubs) {
    mds_rank_t export_pin = cd->inode->get_export_pin();

    if (print_auth_subtrees) {
      dout(25) << "auth tree " << *cd << " export_pin=" << export_pin <<
		  dendl;
    }

    if (export_pin >= 0 && export_pin < mds->mdsmap->get_max_mds()) {
      if (export_pin == mds->get_nodeid()) {
        cd->get_inode()->check_pin_policy();
      } else {
        mds->mdcache->migrator->export_dir(cd, export_pin);
      }
    }
  }
}

void MDBalancer::tick()
{
  static int num_bal_times = g_conf()->mds_bal_max;
  auto bal_interval = g_conf().get_val<int64_t>("mds_bal_interval");
  auto bal_max_until = g_conf().get_val<int64_t>("mds_bal_max_until");
  time now = clock::now();

  if (g_conf()->mds_bal_export_pin) {
    handle_export_pins();
  }

  // sample?
  if (chrono::duration<double>(now-last_sample).count() >
    g_conf()->mds_bal_sample_interval) {
    dout(15) << "tick last_sample now " << now << dendl;
    last_sample = now;
  }

  // We can use duration_cast below, although the result is an int,
  // because the values from g_conf are also integers.
  // balance?
  if (mds->get_nodeid() == 0
      && mds->is_active()
      && bal_interval > 0
      && duration_cast<chrono::seconds>(now - last_heartbeat).count() >= bal_interval
      && (num_bal_times || (bal_max_until >= 0 && mds->get_uptime().count() > bal_max_until))) {
    last_heartbeat = now;
    send_heartbeat();
    
    //MDS0 will not send empty IF
    //send_ifbeat();
    
    num_bal_times--;
  }

  mds->mdcache->show_subtrees(10, true);
}

class C_Bal_SendIFbeat : public MDSInternalContext {
  mds_rank_t target;
  double if_beate_value;
  vector<migration_decision_t> my_decision;
public:
  explicit C_Bal_SendIFbeat(MDSRank *mds_, mds_rank_t target, double if_beate_value, vector<migration_decision_t> migration_decision) : MDSInternalContext(mds_) 
  {
    this->target = target;
    this->if_beate_value = if_beate_value;
    this->my_decision.assign(migration_decision.begin(),migration_decision.end());
  }
  void finish(int f) override {
    mds->balancer->send_ifbeat(target,if_beate_value,my_decision);
  }
};

class C_Bal_SendHeartbeat : public MDSInternalContext {
public:
  explicit C_Bal_SendHeartbeat(MDSRank *mds_) : MDSInternalContext(mds_) { }
  void finish(int f) override {
    mds->balancer->send_heartbeat();
  }
};


double mds_load_t::mds_pop_load()
{
  switch(g_conf()->mds_bal_mode) {
  case 0:
    return
      .8 * auth.meta_load() +
      .2 * all.meta_load() +
      req_rate +
      10.0 * queue_len;

  case 1:
    return req_rate + 10.0*queue_len;

  case 2:
    return cpu_load_avg;

  }
  ceph_abort();
  return 0;
}

double mds_load_t::mds_pot_load(bool auth, int epoch)
{
  if (auth) return pot_auth.pot_load(epoch);

  return
    .8 * pot_auth.pot_load(epoch) +
    .2 * pot_all.pot_load(epoch);
}

double mds_load_t::mds_load(double alpha, double beta, int epoch, bool is_auth, MDBalancer * bal)
{

  if (is_auth)
    //return alpha * auth.meta_load(bal->rebalance_time, bal->mds->mdcache->decayrate) + beta * pot_auth.pot_load(epoch);
    return alpha * auth.meta_load(bal->rebalance_time, bal->mds->mdcache->decayrate) + beta * pot_all.pot_load(epoch);
  else
    return alpha * mds_pop_load() + beta * mds_pot_load(epoch);
}

mds_load_t MDBalancer::get_load()
{
  auto now = clock::now();

  mds_load_t load{DecayRate()}; /* zero DecayRate! */

  if (mds->mdcache->get_root()) {
    auto&& ls = mds->mdcache->get_root()->get_dirfrags();
    for (auto &d : ls) {
      load.auth.add(d->pop_auth_subtree_nested);
      load.all.add(d->pop_nested);
      load.pot_auth.add(d->pot_auth);
      load.pot_all.add(d->pot_all);
    }
  } else {
    dout(20) << "no root, no load" << dendl;
  }

  uint64_t num_requests = mds->get_num_requests();

  uint64_t cpu_time = 1;
  {
    string stat_path = PROCPREFIX "/proc/self/stat";
    ifstream stat_file(stat_path);
    if (stat_file.is_open()) {
      vector<string> stat_vec(std::istream_iterator<string>{stat_file},
			      std::istream_iterator<string>());
      if (stat_vec.size() >= 15) {
	// utime + stime
	cpu_time = strtoll(stat_vec[13].c_str(), nullptr, 10) +
		   strtoll(stat_vec[14].c_str(), nullptr, 10);
      } else {
	derr << "input file '" << stat_path << "' not resolvable" << dendl_impl;
      }
    } else {
      derr << "input file '" << stat_path << "' not found" << dendl_impl;
    }
  }

  load.queue_len = messenger->get_dispatch_queue_len();

  bool update_last = true;
  if (last_get_load != clock::zero() &&
      now > last_get_load) {
    double el = std::chrono::duration<double>(now-last_get_load).count();
    if (el >= 1.0) {
      if (num_requests > last_num_requests)
	load.req_rate = (num_requests - last_num_requests) / el;
      if (cpu_time > last_cpu_time)
	load.cpu_load_avg = (cpu_time - last_cpu_time) / el;
    } else {
      auto p = mds_load.find(mds->get_nodeid());
      if (p != mds_load.end()) {
	load.req_rate = p->second.req_rate;
	load.cpu_load_avg = p->second.cpu_load_avg;
      }
      if (num_requests >= last_num_requests && cpu_time >= last_cpu_time)
	update_last = false;
    }
  }

  if (update_last) {
    last_num_requests = num_requests;
    last_cpu_time = cpu_time;
    last_get_load = now;
  }

  dout(15) << load << dendl;
  return load;
}

/*
 * Read synchronously from RADOS using a timeout. We cannot do daemon-local
 * fallbacks (i.e. kick off async read when we are processing the map and
 * check status when we get here) with the way the mds is structured.
 */
int MDBalancer::localize_balancer()
{
  /* reset everything */
  bool ack = false;
  int r = 0;
  bufferlist lua_src;
  ceph::mutex lock = ceph::make_mutex("lock");
  ceph::condition_variable cond;

  /* we assume that balancer is in the metadata pool */
  object_t oid = object_t(mds->mdsmap->get_balancer());
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  ceph_tid_t tid = mds->objecter->read(oid, oloc, 0, 0, CEPH_NOSNAP, &lua_src, 0,
                                       new C_SafeCond(lock, cond, &ack, &r));
  dout(15) << "launched non-blocking read tid=" << tid
           << " oid=" << oid << " oloc=" << oloc << dendl;

  /* timeout: if we waste half our time waiting for RADOS, then abort! */
  std::cv_status ret_t = [&] {
    auto bal_interval = g_conf().get_val<int64_t>("mds_bal_interval");
    std::unique_lock locker{lock};
    return cond.wait_for(locker, std::chrono::seconds(bal_interval / 2));
  }();
  /* success: store the balancer in memory and set the version. */
  if (!r) {
    if (ret_t == std::cv_status::timeout) {
      mds->objecter->op_cancel(tid, -ECANCELED);
      return -ETIMEDOUT;
    }
    bal_code.assign(lua_src.to_str());
    bal_version.assign(oid.name);
    dout(10) "bal_code=" << bal_code << dendl;
  }
  return r;
}

void MDBalancer::send_ifbeat(mds_rank_t target, double if_beate_value, vector<migration_decision_t>& migration_decision){
  utime_t now = ceph_clock_now();
  mds_rank_t whoami = mds->get_nodeid();
  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (0) Prepare to send ifbeat: " << if_beate_value << " from " << whoami << " to " << target << dendl;
  if (mds->is_cluster_degraded()) {
    dout(10) << "send_ifbeat degraded" << dendl;
    return;
  }

  if (!mds->mdcache->is_open()) {
    dout(5) << "not open" << dendl;
    vector<migration_decision_t> &waited_decision(migration_decision);
    //vector<migration_decision_t> &waited_decision1 = migration_decision;
    mds->mdcache->wait_for_open(new C_Bal_SendIFbeat(mds,target,if_beate_value, waited_decision));
    return;
  }

  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (1) OK, could send ifbeat" << dendl;
  
  set<mds_rank_t> up;
  mds->get_mds_map()->get_up_mds_set(up);
  
  //myload
  mds_load_t load = get_load(now);
  set<mds_rank_t>::iterator target_mds=up.find(target);

  if(target_mds==up.end()){
  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (1.1) ERR: Can't find MDS"<< *target_mds << dendl;
  return;
  }

  MIFBeat *ifm = new MIFBeat(load, beat_epoch, if_beate_value, migration_decision);
  messenger->send_message(ifm,
                            mds->mdsmap->get_inst(*target_mds));
}

void MDBalancer::send_heartbeat()
{
  if (mds->is_cluster_degraded()) {
    dout(10) << "degraded" << dendl;
    return;
  }

  if (!mds->mdcache->is_open()) {
    dout(10) << "not open" << dendl;
    mds->mdcache->wait_for_open(new C_Bal_SendHeartbeat(mds));
    return;
  }

  map<mds_rank_t, mds_load_t>::iterator it = mds_load.begin();
  #ifdef MDS_MONITOR
  while(it != mds_load.end()){
    dout(7) << " MDS_MONITOR " << __func__ << " (1) before send hearbeat, retain mds_load <" << it->first << "," << it->second << ">" << dendl;
    it++; 
  }
  #endif

  if (mds->get_nodeid() == 0) {
    beat_epoch++;

    req_tracer.switch_epoch();
   
    mds_load.clear();
  }

  // my load
  mds_load_t load = get_load();
  mds->logger->set(l_mds_load_cent, 100 * load.mds_load());
  mds->logger->set(l_mds_dispatch_queue_len, load.queue_len);

  auto em = mds_load.emplace(std::piecewise_construct, std::forward_as_tuple(mds->get_nodeid()), std::forward_as_tuple(load));
  if (!em.second) {
    em.first->second = load;
  }

  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (2) count my load, MDS." << mds->get_nodeid() << " Load " << load << dendl;
  #endif

  // import_map -- how much do i import from whom

  map<mds_rank_t, float> import_map;
  for (auto& im : mds->mdcache->get_auth_subtrees()) {
    mds_rank_t from = im->inode->authority().first;
    if (from == mds->get_nodeid()) continue;
    if (im->get_inode()->is_stray()) continue;
    //import_map[from] += im->pop_auth_subtree.meta_load();
    import_map[from] += im->get_load(this);
  }
  mds_import_map[ mds->get_nodeid() ] = import_map;


  dout(3) << " epoch " << beat_epoch << " load " << load << dendl;
  for (const auto& [rank, load] : import_map) {
    dout(5) << "  import_map from " << rank << " -> " << load << dendl;
  }
  //#endif
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (3) count imported directory meta_load" << dendl;
  map<mds_rank_t, float>::iterator it_import = import_map.begin();
  while(it_import != import_map.end()){
    dout(7) << " MDS_MONITOR " << __func__ << " (3) from mds " << it_import->first << " meta_load " << it_import->second << dendl;
    it_import++;
  }
  #endif

  // dout(5) << "mds." << mds->get_nodeid() << " epoch " << beat_epoch << " load " << load << dendl;
  // for (map<mds_rank_t, float>::iterator it = import_map.begin();
  //      it != import_map.end();
  //      ++it) {
  //   dout(5) << "  import_map from " << it->first << " -> " << it->second << dendl;
  // }


  set<mds_rank_t> up;
  mds->get_mds_map()->get_up_mds_set(up);
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (4) collect up mds" << dendl;
  set<mds_rank_t>::iterator it_up = up.begin();
  while( it_up != up.end()){
    dout(7) << " MDS_MONITOR " << __func__ << " (4) up mds." << *it_up << dendl;
    it_up++;
  } 
  #endif
  for (const auto& r : up) {
    if (r == mds->get_nodeid())
      continue;
    auto hb = make_message<MHeartbeat>(load, beat_epoch);
    hb->get_import_map() = import_map;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << " (5) send heartbeat to mds." << r << dendl;
    #endif
    mds->send_message_mds(hb, r);
  }

  // done
 out:
  m->put();
}

//handle imbalancer factor message
void MDBalancer::handle_ifbeat(MIFBeat *m){
  mds_rank_t who = mds_rank_t(m->get_source().num());
  mds_rank_t whoami = mds->get_nodeid();
  double simple_migration_amount = 0.2;
  double simple_if_threshold = g_conf->mds_bal_ifthreshold;

  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (1) get ifbeat " << m->get_beat() << " from " << who << " to " << whoami << " load: " << m->get_load() << " IF: " << m->get_IFvaule() << dendl;
  
  if (!mds->is_active())
    goto out;

  if (!mds->mdcache->is_open()) {
    dout(10) << "opening root on handle_ifbeat" << dendl;
    mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (mds->is_cluster_degraded()) {
    dout(10) << " degraded, ignoring" << dendl;
    goto out;
  }

  if(whoami == 0){
    // mds0 is responsible for calculating IF
    if(m->get_beat()!=beat_epoch){
    dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " ifbeat with wrong epoch: " << m->get_beat() << " from " << who << " to " << whoami << dendl;
      return;
    }else{
      // set mds_load[who]
      
      typedef map<mds_rank_t, mds_load_t> mds_load_map_t;
      mds_load_map_t::value_type val(who, m->get_load());
      mds_load.insert(val);
    }

    if(mds->get_mds_map()->get_num_in_mds()==mds_load.size()){
      //calculate IF
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2)  ifbeat: Try to calculate IF " << dendl;
      unsigned cluster_size = mds->get_mds_map()->get_num_in_mds();
      
      
      //vector < map<string, double> > metrics (cluster_size);
      vector <double> IOPSvector(cluster_size);
      vector <double> load_vector(cluster_size);
      for (mds_rank_t i=mds_rank_t(0);
       i < mds_rank_t(cluster_size);
       i++) {
        map<mds_rank_t, mds_load_t>::iterator it = mds_load.find(i);
        if(it==mds_load.end()){
          derr << " cant find target load of MDS." << i << dendl_impl;
          assert(0 == " cant find target load of MDS.");
        }

        if(old_req.find(i)!=old_req.end()){
          IOPSvector[i] = (it->second.req_rate - old_req[i])/g_conf->mds_bal_interval;
        }else{
          //MDS just started, so skip this time
          IOPSvector[i] = 0;
        }
        old_req[i] = it->second.req_rate;
        
        load_vector[i] = calc_mds_load(it->second, true);
        //load_vector[i] = it->second.auth.meta_load();
        /* mds_load_t &load(it->second);
        no need to get all info?
        metrics[i] = {{"auth.meta_load", load.auth.meta_load()},
                      {"all.meta_load", load.all.meta_load()},
                      {"req_rate", load.req_rate},
                      {"queue_len", load.queue_len},
                      {"cpu_load_avg", load.cpu_load_avg}};
        IOPSvector[i] = load.auth.meta_load();*/
        }

      //ok I know all IOPS, know get to calculateIF
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2) get IOPS: " << IOPSvector << " load: "<< load_vector << dendl;
      
      double avg_load = std::accumulate(std::begin(load_vector), std::end(load_vector), 0.0)/load_vector.size(); 
      double avg_IOPS = std::accumulate(std::begin(IOPSvector), std::end(IOPSvector), 0.0)/IOPSvector.size(); 
      double max_IOPS = *max_element(IOPSvector.begin(), IOPSvector.end());
      double sum_quadratic = 0.0;
      unsigned max_exporter_count = 3;
      double my_if_threshold = simple_if_threshold/min(cluster_size,max_exporter_count);
      int importer_count = 0;

      mds_rank_t max_pos = mds_rank_t(IOPSvector.begin() - max_element(IOPSvector.begin(), IOPSvector.end()));
      mds_rank_t min_pos = mds_rank_t(IOPSvector.begin() - min_element(IOPSvector.begin(), IOPSvector.end()));
      mds_rank_t temp_pos=mds_rank_t(0);
      vector<imbalance_summary_t> my_imbalance_vector(cluster_size);
      vector<imbalance_summary_t>::iterator my_if_it = my_imbalance_vector.begin();


      std::for_each (std::begin(IOPSvector), std::end(IOPSvector), [&](const double my_IOPS) {  
        sum_quadratic  += (my_IOPS-avg_IOPS)*(my_IOPS-avg_IOPS);  

        (*my_if_it).my_if = sqrt((my_IOPS-avg_IOPS)*(my_IOPS-avg_IOPS)/(cluster_size-1)) /(sqrt(cluster_size)*avg_IOPS);
        (*my_if_it).my_urgency = 1/(1+pow(exp(1), 5-10*(my_IOPS/g_conf->mds_bal_presetmax)));
        (*my_if_it).my_iops = my_IOPS ? my_IOPS :1;
        (*my_if_it).whoami = temp_pos;
        if(my_IOPS>avg_IOPS){
          (*my_if_it).is_bigger = true;
        }else{
          (*my_if_it).is_bigger = false;
          if((*my_if_it).my_if >= my_if_threshold){
            importer_count++;
          }
        }
        temp_pos++;
        my_if_it++;
      });
      
      importer_count = max(importer_count, 1);
      importer_count = min(importer_count, 5);

      //simple_migration_amount = simple_migration_total_amount / importer_count;
      simple_migration_amount = 0.25;

      double stdev_IOPS = sqrt(sum_quadratic/(IOPSvector.size()-1));
      double imbalance_degree = 0.0;
      
      double urgency = 1/(1+pow(exp(1), 5-10*(max_IOPS/g_conf->mds_bal_presetmax)));

      if(sqrt(IOPSvector.size())*avg_IOPS == 0){
        imbalance_degree = 0.0;
      }else{
        imbalance_degree = stdev_IOPS/(sqrt(IOPSvector.size())*avg_IOPS);
      }

      double imbalance_factor = imbalance_degree*urgency;
      
      //dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.1) avg_IOPS: " << avg_IOPS << " max_IOPS: " << max_IOPS << " stdev_IOPS: " << stdev_IOPS << " imbalance_degree: " << imbalance_degree << " presetmax: " << g_conf->mds_bal_presetmax << dendl;

      

            if(imbalance_factor>=simple_if_threshold){
        dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.1) imbalance_factor is high enough: " << imbalance_factor << " imbalance_degree: " << imbalance_degree << " urgency: " << urgency << " start to send " << dendl;
        
        set<mds_rank_t> up;
        mds->get_mds_map()->get_up_mds_set(up);
        
        std::sort (my_imbalance_vector.begin()+1, my_imbalance_vector.end(), sortImporter);

        for (set<mds_rank_t>::iterator p = up.begin(); p != up.end(); ++p) {
	  if (*p == 0)continue;
        simple_migration_amount = LUNULE_MIG_AMOUNT;
        dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2.01) I'm: " <<  my_imbalance_vector[*p].whoami << " " <<  my_imbalance_vector[*p].my_if << " " << my_imbalance_vector[*p].is_bigger << dendl;
          vector<migration_decision_t> mds_decision;
          if((max_pos == my_imbalance_vector[*p].whoami || my_imbalance_vector[*p].my_if>my_if_threshold) && my_imbalance_vector[*p].is_bigger){
          int max_importer_count = 0;
          int send_num = my_imbalance_vector[*p].whoami;

            for (vector<imbalance_summary_t>::iterator my_im_it = my_imbalance_vector.begin();my_im_it!=my_imbalance_vector.end() && (max_importer_count < max_exporter_count);my_im_it++){
            if((*my_im_it).whoami != send_num &&(*my_im_it).is_bigger == false && ((*my_im_it).my_if >=my_if_threshold  || (*my_im_it).whoami == min_pos )){
              
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2.111), try match mds" << send_num << ", load: " << IOPSvector[send_num] << " with " << (*my_im_it).whoami << " if: " <<  (*my_im_it).my_if << " is_bigger: " << (*my_im_it).is_bigger << " IOPSvector[(*my_im_it).whoami]: " << IOPSvector[(*my_im_it).whoami] << " (*my_im_it).my_iops: " << (*my_im_it).my_iops << dendl;
              
              if(IOPSvector[send_num]<=1){
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " [ERR] Wrong load! of 0:" << IOPSvector[send_num] << dendl;
                return;
              }

              float decision_percent = static_cast<float>(simple_migration_amount*((IOPSvector[*p]-IOPSvector[(*my_im_it).whoami])/IOPSvector[*p]));
              migration_decision_t temp_decision = {(*my_im_it).whoami,static_cast<float>(decision_percent*load_vector[*p]), decision_percent};
              mds_decision.push_back(temp_decision);
              max_importer_count ++;
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2.1) decision: " << temp_decision.target_import_mds << " " << temp_decision.target_export_load  << temp_decision.target_export_percent<< dendl;
              dout(LUNULE_DEBUG_LEVEL) << __func__ << "[Decision] Send decision: (*my_im_it).whoami is: "<< (*my_im_it).whoami<< " send_num is: " << send_num << " amount: " << temp_decision.target_export_percent << " target: " << temp_decision.target_import_mds << dendl;
              simple_migration_amount = simple_migration_amount/2;
              if(simple_migration_amount<=my_if_threshold)break;
            }
          }
          send_ifbeat(send_num, imbalance_factor, mds_decision);
          }
        }

        if( (max_pos == my_imbalance_vector[0].whoami || my_imbalance_vector[0].my_if>my_if_threshold) && my_imbalance_vector[0].is_bigger){
          vector<migration_decision_t> my_decision;
          simple_migration_amount = LUNULE_MIG_AMOUNT;
          int max_importer_count = 0;
          for (vector<imbalance_summary_t>::iterator my_im_it = my_imbalance_vector.begin();my_im_it!=my_imbalance_vector.end() && (max_importer_count < max_exporter_count);my_im_it++){
            if((*my_im_it).whoami != whoami &&(*my_im_it).is_bigger == false && ((*my_im_it).my_if >=(my_if_threshold) || (*my_im_it).whoami == min_pos )){
              //migration_decision_t temp_decision = {(*my_im_it).whoami,static_cast<float>(simple_migration_amount*IOPSvector[0])};
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2.011), try match mds" << 0 << ", load: " << IOPSvector[0] << " with " << (*my_im_it).whoami << " if: " <<  (*my_im_it).my_if << " is_bigger: " << (*my_im_it).is_bigger << " IOPSvector[(*my_im_it).whoami]: " << IOPSvector[(*my_im_it).whoami] << " (*my_im_it).my_iops: " << (*my_im_it).my_iops << dendl;
              if(IOPSvector[0]<=1){
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " [ERR] Wrong load! of 0:" << IOPSvector[0] << dendl;
                return;
              }
              float decision_percent = static_cast<float>(simple_migration_amount*((IOPSvector[0]-IOPSvector[(*my_im_it).whoami])/IOPSvector[0]));
              migration_decision_t temp_decision = {(*my_im_it).whoami,static_cast<float>(decision_percent*load_vector[0]),decision_percent};
              my_decision.push_back(temp_decision);
              max_importer_count ++;
              dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2.2) decision of mds0 to: " << temp_decision.target_import_mds << ", send: " << temp_decision.target_export_load << " percent: " << temp_decision.target_export_percent<< dendl;
              simple_migration_amount = simple_migration_amount/2;
              if(simple_migration_amount<=my_if_threshold)break;
            }
          }
          if(urgency<=0.1){
            dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << "wird bug, dont clear" <<dendl;
          }else{
            dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << "new epoch, clear_export_queue" <<dendl;
            if(beat_epoch%2==0){
            mds->mdcache->migrator->clear_export_queue();  
            }
          }
          simple_determine_rebalance(my_decision);
        }
      }else{
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.2) imbalance_factor is low: " << imbalance_factor << " imbalance_degree: " << imbalance_degree << " urgency: " << urgency << dendl;
      //mds->mdcache->migrator->clear_export_queue();
      }
    }else{
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (3)  ifbeat: No enough MDSload to calculate IF, skip " << dendl;
    }
  }else{
    double get_if_value = m->get_IFvaule();
    if(get_if_value>=simple_if_threshold){
      //dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << "new epoch, clear_export_queue" <<dendl;
      //mds->mdcache->migrator->clear_export_queue();  
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (3.1) Imbalance Factor is high enough: " << m->get_IFvaule() << dendl;
      simple_determine_rebalance(m->get_decision());
      
    }else{
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (3.1) Imbalance Factor is low: " << m->get_IFvaule() << dendl;
      //mds->mdcache->migrator->clear_export_queue();
    }
  }

  // done
 out:
  m->put();
}

void MDBalancer::handle_heartbeat(const cref_t<MHeartbeat> &m)
{
  mds_rank_t who = mds_rank_t(m->get_source().num());
  dout(25) << "=== got heartbeat " << m->get_beat() << " from " << m->get_source().num() << " " << m->get_load() << dendl;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (1) get heartbeat " << m->get_beat() << " from " << who << " load " << m->get_load() << dendl;
  #endif

  if (!mds->is_active())
    return;

  if (!mds->mdcache->is_open()) {
    dout(10) << "opening root on handle_heartbeat" << dendl;
    mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (mds->is_cluster_degraded()) {
    dout(10) << " degraded, ignoring" << dendl;
    return;
  }

  if (mds->get_nodeid() != 0 && m->get_beat() > beat_epoch) {
    dout(10) << "receive next epoch " << m->get_beat() << " from mds." << who << " before mds0" << dendl;

    beat_epoch = m->get_beat();
    // clear the mds load info whose epoch is less than beat_epoch 
    mds_load.clear();
  }

  if (who == 0) {
    dout(20) << " from mds0, new epoch " << m->get_beat() << dendl;
    if (beat_epoch != m->get_beat()) {
      beat_epoch = m->get_beat();
      mds_load.clear();
    }

    send_heartbeat();

    //req_tracer.switch_epoch();

    vector<migration_decision_t> empty_decision;
    send_ifbeat(0, -1, empty_decision);

    mds->mdcache->show_subtrees();
  } else if (mds->get_nodeid() == 0) {
    if (beat_epoch != m->get_beat()) {
      dout(10) << " old heartbeat epoch, ignoring" << dendl;
      return;
    }
  }

  {
    auto em = mds_load.emplace(std::piecewise_construct, std::forward_as_tuple(who), std::forward_as_tuple(m->get_load()));
    if (!em.second) {
      em.first->second = m->get_load();
    }
  }
  mds_import_map[who] = m->get_import_map();

  //if imbalance factor is enabled, won't use old migration
  
  if(g_conf->mds_bal_ifenable == 0){
    unsigned cluster_size = mds->get_mds_map()->get_num_in_mds();
    if (mds_load.size() == cluster_size) {
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (2) receive all mds heartbeats, now start balance" << dendl;
      #endif
      // let's go!
      //export_empties();  // no!

      /* avoid spamming ceph -w if user does not turn mantle on */
      if (mds->mdsmap->get_balancer() != "") {
        int r = mantle_prep_rebalance();
        if (!r) return;
	mds->clog->warn() << "using old balancer; mantle failed for "
                          << "balancer=" << mds->mdsmap->get_balancer()
                          << " : " << cpp_strerror(r);
      }
      prep_rebalance(m->get_beat());
    }
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << " (2) waiting other heartbeats..." << dendl;
    #endif
  }else{
    //use new migration
  }
}

double MDBalancer::try_match(balance_state_t& state, mds_rank_t ex, double& maxex,
                             mds_rank_t im, double& maxim)
{
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " Try Match BEGIN " << dendl;
  #endif
  if (maxex <= 0 || maxim <= 0) return 0.0;

  double howmuch = std::min(maxex, maxim);

  dout(5) << "   - mds." << ex << " exports " << howmuch << " to mds." << im << dendl;

  if (ex == mds->get_nodeid())
    state.targets[im] += howmuch;

  state.exported[ex] += howmuch;
  state.imported[im] += howmuch;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (1) howmuch matched : "<< howmuch << dendl;
  #endif
  maxex -= howmuch;
  maxim -= howmuch;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " Try Match END " << dendl;
  #endif
  return howmuch;
}

void MDBalancer::queue_split(const CDir *dir, bool fast)
{
  dout(10) << __func__ << " enqueuing " << *dir
                       << " (fast=" << fast << ")" << dendl;

  const dirfrag_t frag = dir->dirfrag();

  auto callback = [this, frag](int r) {
    if (split_pending.erase(frag) == 0) {
      // Someone beat me to it.  This can happen in the fast splitting
      // path, because we spawn two contexts, one with mds->timer and
      // one with mds->queue_waiter.  The loser can safely just drop
      // out.
      return;
    }

    CDir *split_dir = mds->mdcache->get_dirfrag(frag);
    if (!split_dir) {
      dout(10) << "drop split on " << frag << " because not in cache" << dendl;
      return;
    }
    if (!split_dir->is_auth()) {
      dout(10) << "drop split on " << frag << " because non-auth" << dendl;
      return;
    }

    // Pass on to MDCache: note that the split might still not
    // happen if the checks in MDCache::can_fragment fail.
    dout(10) << __func__ << " splitting " << *split_dir << dendl;
    mds->mdcache->split_dir(split_dir, g_conf()->mds_bal_split_bits);
  };

  bool is_new = false;
  if (split_pending.count(frag) == 0) {
    split_pending.insert(frag);
    is_new = true;
  }

  if (fast) {
    // Do the split ASAP: enqueue it in the MDSRank waiters which are
    // run at the end of dispatching the current request
    mds->queue_waiter(new MDSInternalContextWrapper(mds, 
          new LambdaContext(std::move(callback))));
  } else if (is_new) {
    // Set a timer to really do the split: we don't do it immediately
    // so that bursts of ops on a directory have a chance to go through
    // before we freeze it.
    mds->timer.add_event_after(bal_fragment_interval,
                               new LambdaContext(std::move(callback)));
  }
}

void MDBalancer::queue_merge(CDir *dir)
{
  const auto frag = dir->dirfrag();
  auto callback = [this, frag](int r) {
    ceph_assert(frag.frag != frag_t());

    // frag must be in this set because only one context is in flight
    // for a given frag at a time (because merge_pending is checked before
    // starting one), and this context is the only one that erases it.
    merge_pending.erase(frag);

    CDir *dir = mds->mdcache->get_dirfrag(frag);
    if (!dir) {
      dout(10) << "drop merge on " << frag << " because not in cache" << dendl;
      return;
    }
    ceph_assert(dir->dirfrag() == frag);

    if(!dir->is_auth()) {
      dout(10) << "drop merge on " << *dir << " because lost auth" << dendl;
      return;
    }

    dout(10) << "merging " << *dir << dendl;

    CInode *diri = dir->get_inode();

    frag_t fg = dir->get_frag();
    while (fg != frag_t()) {
      frag_t sibfg = fg.get_sibling();
      auto&& [complete, sibs] = diri->get_dirfrags_under(sibfg);
      if (!complete) {
        dout(10) << "  not all sibs under " << sibfg << " in cache (have " << sibs << ")" << dendl;
        break;
      }
      bool all = true;
      for (auto& sib : sibs) {
        if (!sib->is_auth() || !sib->should_merge()) {
          all = false;
          break;
        }
      }
      if (!all) {
        dout(10) << "  not all sibs under " << sibfg << " " << sibs << " should_merge" << dendl;
        break;
      }
      dout(10) << "  all sibs under " << sibfg << " " << sibs << " should merge" << dendl;
      fg = fg.parent();
    }

    if (fg != dir->get_frag())
      mds->mdcache->merge_dir(diri, fg);
  };

  if (merge_pending.count(frag) == 0) {
    dout(20) << " enqueued dir " << *dir << dendl;
    merge_pending.insert(frag);
    mds->timer.add_event_after(bal_fragment_interval,
        new LambdaContext(std::move(callback)));
  } else {
    dout(20) << " dir already in queue " << *dir << dendl;
  }
}

void MDBalancer::prep_rebalance(int beat)
{
  balance_state_t state;

  if (g_conf()->mds_thrash_exports) {
    //we're going to randomly export to all the mds in the cluster
    set<mds_rank_t> up_mds;
    mds->get_mds_map()->get_up_mds_set(up_mds);
    for (const auto &rank : up_mds) {
      state.targets[rank] = 0.0;
    }
  } else {
    int cluster_size = mds->get_mds_map()->get_num_in_mds();
    mds_rank_t whoami = mds->get_nodeid();
    rebalance_time = clock::now();

    dout(7) << "cluster loads are" << dendl;

    mds->mdcache->migrator->clear_export_queue();

    // rescale!  turn my mds_load back into meta_load units
    double load_fac = 1.0;
    map<mds_rank_t, mds_load_t>::iterator m = mds_load.find(whoami);
    if ((m != mds_load.end()) && (calc_mds_load(m->second) > 0)) {
      double metald = calc_mds_load(m->second, true);
      double mdsld = calc_mds_load(m->second);
      load_fac = metald / mdsld;
      dout(7) << " load_fac is " << load_fac
	      << " <- " << m->second.auth << " " << metald
	      << " / " << mdsld
	      << dendl;
        #ifdef MDS_MONITOR
        dout(7) << " MDS_MONITOR " << __func__ << " (1) calculate my load factor meta_load " << metald << " mds_load " << mdsld << " load_factor " << load_fac << dendl;
        #endif
    }

    mds_meta_load.clear();

    double total_load = 0.0;
    multimap<double,mds_rank_t> load_map;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << " (2) compute mds cluter load" << dendl;
    #endif
    for (mds_rank_t i=mds_rank_t(0); i < mds_rank_t(cluster_size); i++) {
      mds_load_t& load = mds_load.at(i);

      double l = calc_mds_load(load) * load_fac;
      mds_meta_load[i] = l;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (2) mds." << i << " load " << l << dendl;
      #endif
      if (whoami == 0)
	dout(7) << "  mds." << i
		<< " " << load
		<< " = " << calc_mds_load(load)
		<< " ~ " << l << dendl;

      if (whoami == i) my_load = l;
      total_load += l;

      load_map.insert(pair<double,mds_rank_t>( l, i ));
    }

    // target load
    target_load = total_load / (double)cluster_size;
    dout(7) << "my load " << my_load
	    << "   target " << target_load
	    << "   total " << total_load
	    << dendl;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << " (3) total_load " << total_load << " cluster_size " << cluster_size << " target_load " << target_load << dendl;
    #endif

    // under or over?
    for (const auto& [load, rank] : load_map) {
      if (load < target_load * (1.0 + g_conf()->mds_bal_min_rebalance)) {
        dout(7) << " mds." << rank << " is underloaded or barely overloaded." << dendl;
        mds_last_epoch_under_map[rank] = beat_epoch;
        #ifdef MDS_MONITOR
        dout(7) << " MDS_MONITOR " << __func__ << " (3) my_load is small, so doing nothing (" << my_load << " < " << target_load << " * " <<g_conf->mds_bal_min_rebalance << ")" << dendl;
        #endif
        //last_epoch_under = beat_epoch;
        //mds->mdcache->show_subtrees();
        //return;
      }
    }

    int last_epoch_under = mds_last_epoch_under_map[whoami];
    if (last_epoch_under == beat_epoch) {
      dout(7) << "  i am underloaded or barely overloaded, doing nothing." << dendl;
      return;
    }
    // am i over long enough?
    if (last_epoch_under && beat_epoch - last_epoch_under < 2) {
      dout(7) << "  i am overloaded, but only for " << (beat_epoch - last_epoch_under) << " epochs" << dendl;
      return;
    }

    dout(7) << "  i am sufficiently overloaded" << dendl;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << " (4) I am overloaded!!! Now need to migrate." << dendl;
    dout(7) << " MDS_MONITOR " << __func__ << " (4) decide importer and exporter!" << dendl;
    #endif


    // first separate exporters and importers
    multimap<double,mds_rank_t> importers;
    multimap<double,mds_rank_t> exporters;
    set<mds_rank_t>             importer_set;
    set<mds_rank_t>             exporter_set;

    for (multimap<double,mds_rank_t>::iterator it = load_map.begin();
	 it != load_map.end();
	 ++it) {
      if (it->first < target_load) {
	dout(15) << "   mds." << it->second << " is importer" << dendl;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (4) importer - mds." << it->second << " load " << it->first << " < " << target_load << "(target_load)" <<dendl;
  #endif
	importers.insert(pair<double,mds_rank_t>(it->first,it->second));
	importer_set.insert(it->second);
      } else {
	int mds_last_epoch_under = mds_last_epoch_under_map[it->second];
	if (!(mds_last_epoch_under && beat_epoch - mds_last_epoch_under < 2)) {
	  dout(15) << "   mds." << it->second << " is exporter" << dendl;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (4) exporter - mds." << it->second << " load " << it->first << " >= " << target_load << "(target_load)" <<dendl;
      #endif
	  exporters.insert(pair<double,mds_rank_t>(it->first,it->second));
	  exporter_set.insert(it->second);
	}
      }
    }


    // determine load transfer mapping

    if (true) {
      // analyze import_map; do any matches i can
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (5) determine load transfer mapping " << dendl;
      dout(7) << " MDS_MONITOR " << __func__ << " ----------------------------------- " << dendl;
      dout(7) << " MDS_MONITOR " << __func__ << " (5) before determination, state elements " << dendl;
      map<mds_rank_t, double>::iterator it_target = state.targets.begin();
      while(it_target != state.targets.end()){
        dout(7) << " MDS_MONITOR " << __func__ << "(5) targets mds." << it_target->first << " load " << it_target->second << dendl;
        it_target++;
      }
      map<mds_rank_t, double>::iterator it_import = state.imported.begin();
      while(it_import != state.imported.end()){
        dout(7) << " MDS_MONITOR " << __func__ << "(5) imported mds." << it_import->first << " load " << it_import->second << dendl;
        it_import++;
      }
      map<mds_rank_t, double>::iterator it_export = state.exported.begin();
      while(it_export != state.exported.end()){
        dout(7) << " MDS_MONITOR " << __func__ << "(5) exported mds." << it_export->first << " load " << it_export->second << dendl;
        it_export++;
      }
      dout(7) << " MDS_MONITOR " << __func__ << " ----------------------------------- " << dendl;
      #endif
      dout(15) << "  matching exporters to import sources" << dendl;

      #ifdef MDS_MONITOR
      dout(7) << " BEGIN TO LIST EXPORTER " << dendl;
      #endif
      // big -> small exporters
      for (multimap<double,mds_rank_t>::reverse_iterator ex = exporters.rbegin();
	   ex != exporters.rend();
	   ++ex) {
	double maxex = get_maxex(state, ex->second);
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << "(5) list exporters: "<< maxex << dendl;
    #endif

	if (maxex <= .001) continue;

	// check importers. for now, just in arbitrary order (no intelligent matching).
	for (map<mds_rank_t, float>::iterator im = mds_import_map[ex->second].begin();
	     im != mds_import_map[ex->second].end();
	     ++im) {
	  double maxim = get_maxim(state, im->first);
	  if (maxim <= .001) continue;
	  try_match(state, ex->second, maxex, im->first, maxim);
	  if (maxex <= .001) break;
	}
      }
    }

    // old way
    if (beat % 2 == 1) {
      dout(15) << "  matching big exporters to big importers" << dendl;
      // big exporters to big importers
      multimap<double,mds_rank_t>::reverse_iterator ex = exporters.rbegin();
      multimap<double,mds_rank_t>::iterator im = importers.begin();
      while (ex != exporters.rend() &&
	     im != importers.end()) {
        double maxex = get_maxex(state, ex->second);
	double maxim = get_maxim(state, im->second);
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " before match: maxex: "<<maxex<<" maxim: "<<maxim << dendl;
  #endif
	if (maxex < .001 || maxim < .001) break;
	try_match(state, ex->second, maxex, im->second, maxim);
	if (maxex <= .001) ++ex;
	if (maxim <= .001) ++im;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " after match: maxex: "<<maxex<<" maxim: "<<maxim << dendl;
  #endif
      }
    } else { // new way
      dout(15) << "  matching small exporters to big importers" << dendl;
      // small exporters to big importers
      multimap<double,mds_rank_t>::iterator ex = exporters.begin();
      multimap<double,mds_rank_t>::iterator im = importers.begin();
      while (ex != exporters.end() &&
	     im != importers.end()) {
        double maxex = get_maxex(state, ex->second);
	double maxim = get_maxim(state, im->second);
	if (maxex < .001 || maxim < .001) break;
	try_match(state, ex->second, maxex, im->second, maxim);
	if (maxex <= .001) ++ex;
	if (maxim <= .001) ++im;
      }
    }
  }
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " ----------------------------------- " << dendl;
  dout(7) << " MDS_MONITOR " << __func__ << " (6) after determination, state elements " << dendl;
  map<mds_rank_t, double>::iterator it_target = state.targets.begin();
  while(it_target != state.targets.end()){
    dout(7) << " MDS_MONITOR " << __func__ << "(6) targets mds." << it_target->first << " load " << it_target->second << dendl;
    it_target++;
  }
  map<mds_rank_t, double>::iterator it_import = state.imported.begin();
  while(it_import != state.imported.end()){
    dout(7) << " MDS_MONITOR " << __func__ << "(6) imported mds." << it_import->first << " load " << it_import->second << dendl;
    it_import++;
  }
  map<mds_rank_t, double>::iterator it_export = state.exported.begin();
  while(it_export != state.exported.end()){
    dout(7) << " MDS_MONITOR " << __func__ << "(6) exported mds." << it_export->first << " load " << it_export->second << dendl;
    it_export++;
  }
  dout(7) << " MDS_MONITOR " << __func__ << " ----------------------------------- " << dendl;
  #endif
  try_rebalance(state);
}

int MDBalancer::mantle_prep_rebalance()
{
  balance_state_t state;

  /* refresh balancer if it has changed */
  if (bal_version != mds->mdsmap->get_balancer()) {
    bal_version.assign("");
    int r = localize_balancer();
    if (r) return r;

    /* only spam the cluster log from 1 mds on version changes */
    if (mds->get_nodeid() == 0)
      mds->clog->info() << "mantle balancer version changed: " << bal_version;
  }

  /* prepare for balancing */
  int cluster_size = mds->get_mds_map()->get_num_in_mds();
  rebalance_time = clock::now();
  mds->mdcache->migrator->clear_export_queue();

  /* fill in the metrics for each mds by grabbing load struct */
  vector < map<string, double> > metrics (cluster_size);
  for (mds_rank_t i=mds_rank_t(0); i < mds_rank_t(cluster_size); i++) {
    mds_load_t& load = mds_load.at(i);

    metrics[i] = {{"auth.meta_load", load.auth.meta_load()},
                  {"all.meta_load", load.all.meta_load()},
                  {"req_rate", load.req_rate},
                  {"queue_len", load.queue_len},
                  {"cpu_load_avg", load.cpu_load_avg}};
  }

  /* execute the balancer */
  Mantle mantle;
  int ret = mantle.balance(bal_code, mds->get_nodeid(), metrics, state.targets);
  dout(7) << " mantle decided that new targets=" << state.targets << dendl;

  /* mantle doesn't know about cluster size, so check target len here */
  if ((int) state.targets.size() != cluster_size)
    return -EINVAL;
  else if (ret)
    return ret;

  try_rebalance(state);
  return 0;
}

void MDBalancer::simple_determine_rebalance(vector<migration_decision_t>& migration_decision){

  mds->mdcache->migrator->clear_export_queue();  

  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (1) start to migration by simple policy "<< dendl;
  set<CDir*> already_exporting;
  rebalance_time = ceph_clock_now();
  
  int my_mds_load= calc_mds_load(get_load(rebalance_time), true);
  int sample_count = 0;
  set<CDir*> count_candidates;
  mds->mdcache->get_fullauth_subtrees(count_candidates);

  for (auto &it : migration_decision){
    mds_rank_t target = it.target_import_mds;
    //double ex_load = it.target_export_load;
    
    double ex_load = it.target_export_percent * my_mds_load;

    dout(0) << " MDS_IFBEAT " << __func__ << " (2) want send my_mds_load " << my_mds_load << " * " << it.target_export_percent << " ,to " << target << dendl;
    
    set<CDir*> candidates;
    mds->mdcache->get_fullauth_subtrees(candidates);
    list<CDir*> exports;
    int count = 0;
    double have = 0.0;
    for (set<CDir*>::iterator pot = candidates.begin(); pot != candidates.end(); ++pot) {
      if ((*pot)->is_freezing() || (*pot)->is_frozen() || (*pot)->get_inode()->is_stray()) continue;
      
      //find_exports(*pot, ex_load, exports, have, already_exporting);
      find_exports_wrapper(*pot, ex_load, exports, have, already_exporting, target);
      if(have>= 0.8*ex_load )break;
      /*if(exports.size() - count>=MAX_EXPORT_SIZE)
      {
        count = exports.size();
  dout(0) << " MDS_IFBEAT " << " find: " << exports.size() << " last: " << count << " leave " << target << exports << dendl;       
        break;
      }*/
      //if (have > amount-MIN_OFFLOAD)break;
    }

    dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (2.1) find this: " << exports << dendl;

    for (list<CDir*>::iterator it = exports.begin(); it != exports.end(); ++it) {
      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (3) exporting " << (*it)->pop_auth_subtree << "  " << (*it)->get_load(this) << " to mds." << target << " DIR " << **it <<dendl;
      mds->mdcache->migrator->export_dir_nicely(*it, target);
    }
  }
  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " (4) simple rebalance done" << dendl;
}


void MDBalancer::try_rebalance(balance_state_t& state)
{
  if (g_conf()->mds_thrash_exports) {
    dout(5) << "mds_thrash is on; not performing standard rebalance operation!"
	    << dendl;
    return;
  }

  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR (1) start" <<dendl;
  #endif

  // make a sorted list of my imports
  multimap<double, CDir*> import_pop_map;
  multimap<mds_rank_t, pair<CDir*, double> > import_from_map;

  for (auto& dir : mds->mdcache->get_fullauth_subtrees()) {
    CInode *diri = dir->get_inode();
    if (diri->is_mdsdir())
      continue;
    if (diri->get_export_pin(false) != MDS_RANK_NONE)
      continue;
    if (dir->is_freezing() || dir->is_frozen())
      continue;  // export pbly already in progress

    mds_rank_t from = diri->authority().first;
    double pop = dir->get_load(this);
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR (2) Dir " << *dir << " pop " << pop <<dendl;
    #endif
    if (g_conf()->mds_bal_idle_threshold > 0 &&
	pop < g_conf()->mds_bal_idle_threshold &&
	diri != mds->mdcache->get_root() &&
	from != mds->get_nodeid()) {
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR (2) exporting idle (" << pop << " ) import " << *dir
        << " back to mds. " << diri->authority().first <<dendl;
      #endif
      dout(5) << " exporting idle (" << pop << ") import " << *dir
	      << " back to mds." << from << dendl;
      mds->mdcache->migrator->export_dir_nicely(dir, from);
      continue;
    }

    dout(15) << "  map: i imported " << *dir << " from " << from << dendl;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR (2) record import directory mds." << from << ", Dir " << *dir << " pop " << pop << dendl;
    #endif
    import_pop_map.insert(make_pair(pop, dir));
    import_from_map.insert(make_pair(from, make_pair(dir, pop)));
  }

  // do my exports!
  map<mds_rank_t, double> export_pop_map;

  for (auto &it : state.targets) {
    mds_rank_t target = it.first;
    double amount = it.second;

    if (amount < MIN_OFFLOAD)
      continue;
    if (amount * 10 * state.targets.size() < target_load)
      continue;

    dout(5) << "want to send " << amount << " to mds." << target
      //<< " .. " << (*it).second << " * " << load_fac
	    << " -> " << amount
	    << dendl;//" .. fudge is " << fudge << dendl;

    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << " (3) SELECT exported directory, send " << amount << " to mds." << target <<dendl;
    #endif


    double& have = export_pop_map[target];

    mds->mdcache->show_subtrees();

    // search imports from target
    if (import_from_map.count(target)) {
      dout(7) << " aha, looking through imports from target mds." << target << dendl;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << " (3) aha,  looking through imports from target mds." << target <<dendl;
      #endif
      for (auto p = import_from_map.equal_range(target);
	   p.first != p.second; ) {
	CDir *dir = p.first->second.first;
	double pop = p.first->second.second;
	dout(7) << "considering " << *dir << " from " << (*p.first).first << dendl;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << " (3) considering " << *dir << " from " << (*p.first).first <<dendl;
    #endif
	auto plast = p.first++;

	if (dir->inode->is_base())
	  continue;

	ceph_assert(dir->inode->authority().first == target);  // cuz that's how i put it in the map, dummy
	//if (dir->is_freezing() || dir->is_frozen()) continue;  // export pbly already in progress
	//double pop = dir->get_load(this);

	if (pop <= amount-have) {
	  dout(7) << "reexporting " << *dir << " pop " << pop
		  << " back to mds." << target << dendl;
   
	  mds->mdcache->migrator->export_dir_nicely(dir, target);
	  have += pop;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (3) Have " << have << " reexporting " << *dir << " pop " << pop << " back to mds." << target <<dendl;
      #endif
	  import_from_map.erase(plast);
	  for (auto q = import_pop_map.equal_range(pop);
	       q.first != q.second; ) {
	    if (q.first->second == dir) {
	      import_pop_map.erase(q.first);
	      break;
	    }
	    q.first++;
	  }
	} else {
	  dout(7) << "can't reexport " << *dir << ", too big " << pop << dendl;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << " (3) can't reexport " << *dir << ", too big " << pop << dendl;
      #endif
	}
	if (amount-have < MIN_OFFLOAD)
	  break;
      }
    }
  }

  // any other imports
  for (auto &it : state.targets) {
    mds_rank_t target = it.first;
    double amount = it.second;

    if (!export_pop_map.count(target))
      continue;
    double& have = export_pop_map[target];
    if (amount-have < MIN_OFFLOAD)
      continue;

    for (auto p = import_pop_map.begin();
	 p != import_pop_map.end(); ) {
      CDir *dir = p->second;
      if (dir->inode->is_base()) {
	++p;
	continue;
      }

      double pop = p->first;
      if (pop <= amount-have && pop > MIN_REEXPORT) {
	dout(5) << "reexporting " << *dir << " pop " << pop
		<< " to mds." << target << dendl;
	have += pop;
	mds->mdcache->migrator->export_dir_nicely(dir, target);
	import_pop_map.erase(p++);
      } else {
	++p;
      }
      if (amount-have < MIN_OFFLOAD)
	break;
    }
  }

  // okay, search for fragments of my workload
   #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << " (4) searching directory workloads " <<dendl;
  #endif
  set<CDir*> already_exporting;

  for (auto &it : state.targets) {
    mds_rank_t target = it.first;
    double amount = it.second;

    if (!export_pop_map.count(target))
      continue;
    double& have = export_pop_map[target];
    if (amount-have < MIN_OFFLOAD)
      continue;

    // okay, search for fragments of my workload
    std::vector<CDir*> exports;

    for (auto p = import_pop_map.rbegin();
	 p != import_pop_map.rend();
	 ++p) {
      CDir *dir = p->second;
      find_exports(dir, amount, &exports, have, already_exporting);
      if (amount-have < MIN_OFFLOAD)
	break;
    }
      //find_exports(*pot, amount, exports, have, already_exporting);

    }
    //fudge = amount - have;

    for (const auto& dir : exports) {
      dout(5) << "   - exporting " << dir->pop_auth_subtree
	      << " " << dir->pop_auth_subtree.meta_load()
	      << " to mds." << target << " " << *dir << dendl;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << " (5) exporting " << dir->pop_auth_subtree << "  " << dir->pop_auth_subtree.meta_load()
       << " to mds." << target << " DIR " << *dir << dendl;
      #endif
      mds->mdcache->migrator->export_dir_nicely(dir, target);
    }
  }

  dout(7) << "done" << dendl;
  mds->mdcache->show_subtrees();
}

void MDBalancer::find_exports(CDir *dir,
                              double amount,
                              list<CDir*>& exports,
                              double& have,
                              set<CDir*>& already_exporting,
			      mds_rank_t target)
{
  auto now = clock::now();
  auto duration = std::chrono::duration<double>(now-rebalance_time).count();
  if (duration > 0.1) {
    derr << " balancer runs too long"  << dendl_impl;
    have = amount;
    return;
  }

  ceph_assert(dir->is_auth());

  double need = amount - have;
  if (need < amount * g_conf()->mds_bal_min_start)
    return;   // good enough!

  double needmax = need * g_conf()->mds_bal_need_max;
  double needmin = need * g_conf()->mds_bal_need_min;
  double midchunk = need * g_conf()->mds_bal_midchunk;
  double minchunk = need * g_conf()->mds_bal_minchunk;

  std::vector<CDir*> bigger_rep, bigger_unrep;
  multimap<double, CDir*> smaller;

  double dir_pop = dir->get_load(this);
  
  double minchunk =0.5;
  int frag_mod_dest = 0;
  unsigned int hash_frag = 0;
  unsigned int frag_num = 0;
  std::hash<unsigned> hash_frag_func;
  int cluster_size = mds->get_mds_map()->get_num_in_mds();
  int skip_pos = cluster_size;
  
  dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT find in " << *dir << " pop: " << dir_pop << " Vel: " << dir->pop_auth_subtree.show_meta_vel() << " need " << need << " (" << needmin << " - " << needmax << ")" << dendl;
  //dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT Vel: " << dir->pop_auth_subtree.show_meta_vel()<<dendl;
  dout(7) << "in " << dir_pop << " " << *dir << " need " << need << " (" << needmin << " - " << needmax << ")" << dendl;
  #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR needmax " << needmax << " needmin " << needmin << " midchunk " << midchunk << " minchunk " << minchunk << dendl;
  dout(7) << " MDS_MONITOR (1) Find DIR " << *dir << " expect_request " << dir_pop << 
  " amount " << amount << " have " << have << " need " << need << dendl;
  #endif  

  double subdir_sum = 0;
  for (elist<CInode*>::iterator it = dir->pop_lru_subdirs.begin_use_current();
       !it.end(); ) {
    CInode *in = *it;
    ++it;

    ceph_assert(in->is_dir());
    ceph_assert(in->get_parent_dir() == dir);

    auto&& dfls = in->get_nested_dirfrags();

    size_t num_idle_frags = 0;
    for (const auto& subdir : dfls) {
      if (already_exporting.count(subdir))
	continue;

      // we know all ancestor dirfrags up to subtree root are not freezing or frozen.
      // It's more efficient to use CDir::is_{freezing,frozen}_tree_root()
      if (subdir->is_frozen_dir() || subdir->is_frozen_tree_root() ||
	  subdir->is_freezing_dir() || subdir->is_freezing_tree_root())
	continue;  // can't export this right now!

      // how popular?
      double pop = subdir->get_load(this);
      subdir_sum += pop;
      
      /*if(pop<0){
      string s;
      subdir->get_inode()->make_path_string(s);
      pair<double, double> result = req_tracer.alpha_beta(s, subdir->get_num_dentries_auth_subtree_nested());
      dout(0) << " [Wrong!] minus pop " << pop << " " << *subdir << " potauth: " << subdir->pot_auth << " alpha: " << result.first << " beta: " << result.second << dendl;
      }*/

      dout(LUNULE_DEBUG_LEVEL) << " MDS_IFBEAT " << __func__ << " find in subdir " << *subdir << " pop: " << pop << " have " << have << " Vel: " << subdir->pop_auth_subtree.show_meta_vel() << dendl;

      dout(15) << "   subdir pop " << pop << " " << *subdir << dendl;
      #ifdef MDS_MONITOR
      dout(7) << " MDS_MONITOR " << __func__ << "(2) Searching DIR " << *subdir << " pop " << pop << dendl;
      #endif

      if (pop < minchunk) {
	num_idle_frags++;
	continue;
      }

      /*if(exports.size() - my_exports>=MAX_EXPORT_SIZE)
      {
        dout(LUNULE_DEBUG_LEVEL) << " [WAN]: got" << exports.size() - my_exports << " targets, enough! " << *dir << dendl;
        return;
      }*/

      // lucky find?
      if (pop > needmin && pop < needmax) {
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << "(2) Lucky Find DIR " << *subdir << " pop " << pop << 
    " needmin~needmax " << needmin << " ~ " << needmax << " have " << have << " need " << need << dendl;
    #endif 
	exports->push_back(subdir);
	already_exporting.insert(subdir);

	have += pop;
	return;
      }

      if (pop > need) {
	if (subdir->is_rep())
	  bigger_rep.push_back(subdir);
	else
	  bigger_unrep.push_back(subdir);
      } else{
        if(g_conf->mds_bal_frag == 0){
    //hash mode
    frag_num = subdir->dirfrag().frag.value() >> (24 - subdir->dirfrag().frag.bits());
    hash_frag = hash_frag_func(subdir->dirfrag().ino + frag_num);
    frag_mod_dest = hash_frag%cluster_size;
    
    if(frag_mod_dest == target ){
        smaller.insert(pair<double,CDir*>(pop, subdir));
        skip_pos = 0;
      }else{
        skip_pos +=1;
        if(skip_pos>=(cluster_size+1)){
        smaller.insert(pair<double,CDir*>(pop, subdir));
        skip_pos = 0;
        }
      }
  }else{
    //raw mode
    smaller.insert(pair<double,CDir*>(pop, subdir));

  }

      }
    }
    if (dfls.size() == num_idle_frags)
      in->item_pop_lru.remove_myself();
  }
  dout(15) << "   sum " << subdir_sum << " / " << dir_pop << dendl;

  // grab some sufficiently big small items
  multimap<double,CDir*>::reverse_iterator it=smaller.rbegin();
  multimap<double,CDir*>::iterator big_it=smaller.begin();

  for (it = smaller.rbegin();
       it != smaller.rend();
       ++it) {

    if ((*it).first < midchunk)break;  // try later

    dout(7) << "   taking smaller " << *(*it).second << dendl;
    #ifdef MDS_MONITOR
    dout(0) << " MDS_MONITOR " << __func__ << "(3) taking smaller DIR " << *((*it).second) << " pop " << (*it).first << dendl;
    #endif

    exports->push_back((*it).second);
    already_exporting.insert((*it).second);
    have += (*it).first;
    
    //if(exports.size() - my_exports>=MAX_EXPORT_SIZE)
  {
    dout(LUNULE_DEBUG_LEVEL) << " [WAN]: enough! " << *dir << dendl;
    return;
  }
    if (have > needmin)
      break;
      //break;
  }

  // apprently not enough; drill deeper into the hierarchy (if non-replicated)
  for (const auto& dir : bigger_unrep) {
    dout(15) << "   descending into " << *dir << dendl;
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR " << __func__ << "(4) descending into bigger DIR " << **dir << dendl;
    #endif
    //find_exports(dir, amount, exports, have, already_exporting);
    //find_exports(*it, amount, exports, have, already_exporting);
    find_exports_wrapper(dir, amount, exports, have, already_exporting, target);
    //if (have > need)
    if (have > needmin)
      return;
  }

  // ok fine, use smaller bits
  for (;
       it != smaller.rend();
       ++it) {
    dout(7) << "   taking (much) smaller " << it->first << " " << *(*it).second << dendl;

    #ifdef MDS_MONITOR
  dout(7) << " MDS_MONITOR " << __func__ << "(5) taking (much) smaller DIR " << *((*it).second) << " pop " << (*it).first << dendl;
  #endif
    exports->push_back((*it).second);
    already_exporting.insert((*it).second);
    have += (*it).first;
    if (have > need)
      return;
  }

  // ok fine, drill into replicated dirs
  for (const auto& dir : bigger_rep) {
    #ifdef MDS_MONITOR
    dout(7) << " MDS_MONITOR (6) descending into replicated DIR " << *dir << dendl;
    #endif
    dout(7) << "   descending into replicated " << *dir << dendl;
    find_exports(dir, amount, exports, have, already_exporting);
    //find_exports_wrapper(dir, amount, exports, have, already_exporting, target);
    //if (have > need)
    if (have > needmin)
      return;
  }
}


void MDBalancer::dynamically_fragment(CDir *dir, double amount){
  if( amount <= 0.1){
    dout(LUNULE_DEBUG_LEVEL) << __func__ << " amount to low: " << amount << *dir << dendl;
    return;
  }

  if (dir->get_inode()->is_stray() || !dir->is_auth()) return;
  dout(LUNULE_DEBUG_LEVEL) << __func__ << " dynamically(0): " << *dir << dendl;
  double dir_pop = dir->get_load(this);
  if(dir_pop >amount*0.6){
    dout(LUNULE_DEBUG_LEVEL) << __func__ << " my_pop:  " << dir_pop << " is big enough for: " << amount << *dir << dendl;
    double sub_dir_total = 0;
    for (auto it = dir->begin(); it != dir->end(); ++it) {
    CInode *in = it->second->get_linkage()->get_inode();
    if (!in) continue;
    if (!in->is_dir()) continue;
    dout(LUNULE_DEBUG_LEVEL) << __func__ << " dynamically(1): I'm " << *in << dendl;

    list<CDir*> dfls;

    in->get_dirfrags(dfls);
    for (list<CDir*>::iterator p = dfls.begin();
   p != dfls.end();
   ++p) {
      CDir *subdir = *p;
      double sub_dir_pop = subdir->get_load(this);
      sub_dir_total +=sub_dir_pop;
      }
   }
  double file_hot_ratio = 1-sub_dir_total/dir_pop;
  if(file_hot_ratio >=0.5){
    dout(LUNULE_DEBUG_LEVEL) << __func__ << " dynamically (2) file_access_hotspot " << file_hot_ratio << " find in " << *dir << dendl;
    maybe_fragment(dir,true);
  }
  dout(LUNULE_DEBUG_LEVEL) << __func__ << " dynamically(3) I'm" << *dir << " my_pop: " << dir_pop << " my_sub_pop: " << sub_dir_total << dendl;


  }else{
    return;
  }

   

}

void MDBalancer::find_exports_wrapper(CDir *dir,
                    double amount,
                    list<CDir*>& exports,
                    double& have,
                    set<CDir*>& already_exporting,
		    mds_rank_t target)
{
  if(dir->inode->is_stray())return;
  double total_hot = 0;
  string s;
  dir->get_inode()->make_path_string(s);
  WorkloadType wlt = adsl::workload2type(adsl::g_matcher.match(s));
  dout(LUNULE_DEBUG_LEVEL) << __func__ << " path=" << s << " workload=" << adsl::g_matcher.match(s) << " type=" << wlt << dendl;
  CInode *in = dir->get_inode();
  //dynamically_fragment(dir, amount);
  list<CDir*> dfls;
  list<CDir*> little_dfls;
  switch (wlt) {
    case WLT_ROOT:
      dout(LUNULE_DEBUG_LEVEL) << __func__ << " Root: diving to " << *dir << dendl;
      
      in->get_dirfrags(dfls);
      for (auto child_dir : dfls) {
        child_dir->get_inode()->get_dirfrags(little_dfls);
      for (auto little_child_dir : little_dfls)
      {
        if (little_child_dir->get_load(this) > 0.1* amount)
        {
          dout(0) << __func__ << " Root: diving to little child" << little_child_dir << " from " << child_dir << dendl;
          find_exports(little_child_dir, amount, exports, have, already_exporting, target);
        }else{
          dout(0) << __func__ << " cant diving to little child" << little_child_dir << " load: " << little_child_dir->get_load(this) << " vs " << amount << dendl;
        }
      }
      
    }

      break;
    default:
      if(mds->get_nodeid()==0){
      dout(LUNULE_DEBUG_LEVEL) << __func__ << " Unknown-MDS0: diving to " << *dir << dendl;
      find_exports(dir, amount, exports, have, already_exporting, target);
      }else{
      if (dir->inode->get_parent_dir())
      {
        dout(LUNULE_DEBUG_LEVEL) << __func__ << " Unknown-MDS not 0: diving to parent: " << *(dir->inode->get_parent_dir()) << dendl;
        find_exports(dir->inode->get_parent_dir(), amount, exports, have, already_exporting, target);
      }else{
        dout(LUNULE_DEBUG_LEVEL) << __func__ << " Unknown-MDS not 0: no parent: " << *dir << dendl;
        find_exports(dir, amount, exports, have, already_exporting, target);
      }
      }
      break;
  }
}


void MDBalancer::hit_inode(CInode *in, int type, int who)
{
  // hit inode pop and count
  in->pop.get(type).hit();
  int newold = in->hit(true, beat_epoch);

  if (in->get_parent_dn())
    hit_dir(in->get_parent_dn()->get_dir(), type, who, 1.0, newold);
}


void MDBalancer::maybe_fragment(CDir *dir, bool hot)
{
  //dout(0) << __func__ << " split " << *dir << dendl;
  dout(20) << __func__ << dendl;
  // split/merge
  if (bal_fragment_dirs && bal_fragment_interval > 0 &&
      dir->is_auth() &&
      !dir->inode->is_base() &&  // not root/mdsdir (for now at least)
      !dir->inode->is_stray()) { // not straydir

    // split
    if (g_conf()->mds_bal_split_size > 0 && (dir->should_split() || hot)) {
      //dout(20) << __func__ << " mds_bal_split_size " << g_conf()->mds_bal_split_size << " dir's frag size " << dir->get_frag_size() << dendl;
      if (split_pending.count(dir->dirfrag()) == 0) {
        queue_split(dir, false);
      } else {
        if (dir->should_split_fast()) {
          queue_split(dir, true);
        } else {
          dout(10) << ": fragment already enqueued to split: "
                   << *dir << dendl;
        }
      }
    }

    // merge?
    if (dir->get_frag() != frag_t() && dir->should_merge() &&
	merge_pending.count(dir->dirfrag()) == 0) {
      queue_merge(dir);
    }
  }
}

//auto update_dir_pot_recur = [this] (CDir * dir, int level, double adj_auth_pot = 1.0, double adj_all_pot = 1.0) -> void {
void MDBalancer::update_dir_pot_recur(CDir * dir, int level, double adj_auth_pot, double adj_all_pot)
{
  string s = "/";
  if (dir->inode->get_parent_dn())
    dir->inode->make_path_string(s);
  //dout(0) << __func__ << " path=" << s << " level=" << level << " adj_auth=" << adj_auth_pot << " adj_all=" << adj_all_pot << dendl;

  // adjust myself
  dir->pot_all.adjust(adj_all_pot, beat_epoch);
  if (dir->is_auth())
    dir->pot_auth.adjust(adj_auth_pot, beat_epoch);

  int brocount = 0;
  if (level <= 0)	goto finish;

  for (auto it = dir->begin(); it != dir->end(); it++)
    brocount += dir->get_authsubtree_size_slow(beat_epoch);
  for (auto it = dir->begin(); it != dir->end(); it++) {
    int my_subtree_size = dir->get_authsubtree_size_slow(beat_epoch);
    double my_adj_auth_pot = adj_auth_pot * my_subtree_size / brocount;
    double my_adj_all_pot = adj_all_pot * my_subtree_size / brocount;
    CDentry::linkage_t * de_l = it->second->get_linkage();
    if (de_l && de_l->is_primary()) {
      CInode * in = de_l->get_inode();
      list<CDir *> petals;
      in->get_dirfrags(petals);
      int brothers_count = 1;
      int brothers_auth_count = 1;
      int bro_dir_count = 1;
      int bro_auth_dir_count = 1;
      for (CDir * petal : petals) {
        brothers_count += (1+petal->get_num_any());
        bro_dir_count +=1;
        if (petal->is_auth()){
          brothers_auth_count += (1+petal->get_num_any());
          bro_auth_dir_count +=1;
        }
      }
      double adj_auth_single = brothers_auth_count ? (my_adj_auth_pot / brothers_auth_count) : 0.0;
      double adj_all_single = brothers_count ? (my_adj_all_pot / brothers_count) : 0.0;
      double adj_auth_dir = bro_auth_dir_count ? my_adj_auth_pot/bro_auth_dir_count : 0.0;
      double adj_all_dir = bro_dir_count ? my_adj_all_pot/bro_dir_count : 0.0;
      for (CDir * petal : petals) {
        update_dir_pot_recur(petal, level - 1, 0.5*(1+petal->get_num_any()) * adj_auth_single + 0.5*adj_auth_dir, 0.5*(1+petal->get_num_any()) * adj_all_single + 0.5*adj_all_dir);      }
    }
  }

finish:
  return;
  //dout(0) << __func__ << " after adjust, path=" << s << " level=" << level << " pot_auth=" << dir->pot_auth << " pot_all=" << dir->pot_all << dendl;
}


void MDBalancer::hit_dir(CDir *dir, int type, int who, double amount, int newold)
{
  // hit me
  double v = dir->pop_me.get(type).hit(amount);

  const bool hot = (v > g_conf()->mds_bal_split_rd && type == META_POP_IRD) ||
                   (v > g_conf()->mds_bal_split_wr && type == META_POP_IWR);

  dout(20) << type << " pop is " << v << ", frag " << dir->get_frag()
           << " size " << dir->get_frag_size() << " " << dir->pop_me << dendl;

  maybe_fragment(dir, hot);

  // replicate?
  if (type == META_POP_IRD && who >= 0) {
    dir->pop_spread.hit(who);
  }

  double rd_adj = 0.0;
  if (type == META_POP_IRD &&
      dir->last_popularity_sample < last_sample) {
    double dir_pop = dir->pop_auth_subtree.get(type).get();    // hmm??
    dir->last_popularity_sample = last_sample;
    double pop_sp = dir->pop_spread.get();
    dir_pop += pop_sp * 10;

    //if (dir->ino() == inodeno_t(0x10000000002))
    if (pop_sp > 0) {
      dout(20) << type << " pop " << dir_pop << " spread " << pop_sp
	      << " " << dir->pop_spread.last[0]
	      << " " << dir->pop_spread.last[1]
	      << " " << dir->pop_spread.last[2]
	      << " " << dir->pop_spread.last[3]
	      << " in " << *dir << dendl;
    }

    dout(20) << "TAG hit_dir " << dir->get_path() << " dir_pop " << dir_pop << " mds_bal_replicate_threshold " << g_conf->mds_bal_replicate_threshold << dendl; 
    if (dir->is_auth() && !dir->is_ambiguous_auth()) {
      if (!dir->is_rep() &&
	  dir_pop >= g_conf()->mds_bal_replicate_threshold) {
	// replicate
	double rdp = dir->pop_me.get(META_POP_IRD).get();
	rd_adj = rdp / mds->get_mds_map()->get_num_in_mds() - rdp;
	rd_adj /= 2.0;  // temper somewhat

	dout(5) << "replicating dir " << *dir << " pop " << dir_pop << " .. rdp " << rdp << " adj " << rd_adj << dendl;

	#ifdef MDS_MONITOR
	dout(LUNULE_DEBUG_LEVEL) << "replicating dir " << *dir << " pop " << dir_pop << " .. rdp " << rdp << " adj " << rd_adj << dendl;
	#endif
	
	dir->dir_rep = CDir::REP_ALL;
	mds->mdcache->send_dir_updates(dir, true);

	// fixme this should adjust the whole pop hierarchy
	dir->pop_me.get(META_POP_IRD).adjust(rd_adj);
	dir->pop_auth_subtree.get(META_POP_IRD).adjust(rd_adj);
      }

      if (dir->ino() != 1 &&
	  dir->is_rep() &&
	  dir_pop < g_conf()->mds_bal_unreplicate_threshold) {
	// unreplicate
	dout(5) << "unreplicating dir " << *dir << " pop " << dir_pop << dendl;

	dir->dir_rep = CDir::REP_NONE;
	mds->mdcache->send_dir_updates(dir);
      }
    }
  }

  // adjust ancestors
  bool hit_subtree = dir->is_auth();         // current auth subtree (if any)
  bool hit_subtree_nested = dir->is_auth();  // all nested auth subtrees

  CDir * origdir = dir;
  while (true) {
    CDir *pdir = dir->inode->get_parent_dir();
    dir->pop_nested.get(type).hit(amount);
    if (rd_adj != 0.0)
      dir->pop_nested.get(META_POP_IRD).adjust(rd_adj);

    if (hit_subtree) {
      dir->pop_auth_subtree.get(type).hit(amount);

      if (rd_adj != 0.0)
	dir->pop_auth_subtree.get(META_POP_IRD).adjust(rd_adj);

      if (dir->is_subtree_root())
	hit_subtree = false;                // end of auth domain, stop hitting auth counters.
      else if (pdir)
	pdir->pop_lru_subdirs.push_front(&dir->get_inode()->item_pop_lru);
    }

    if (hit_subtree_nested) {
      dir->pop_auth_subtree_nested.get(type).hit(amount);
      if (rd_adj != 0.0)
	dir->pop_auth_subtree_nested.get(META_POP_IRD).adjust(rd_adj);
    }
    if (!pdir) break;
    dir = pdir;
  }

  if (newold < 1)	return;

  dir = origdir;
  //dout(0) << __func__ << " DEBUG dir=" << dir->get_path() << dendl;
  //CDentry* dn = dir->get_inode()->get_parent_dn();
  //dout(0) << __func__ << " DEBUG2 dir=" << (dn ? dn->get_name() : "/") << dendl;
  // adjust potential load for brother dirfrags
  auto update_dir_pot = [this](CDir * dir, int level = 0) -> bool {
    CInode * in = dir->inode;
    int i;
    for (i = 0; i < level; i++) {
      if (!in->get_parent_dn())	break;
      in = in->get_parent_dn()->get_dir()->get_inode();
    }
    bool ret = (i == level);
    level = i;

    list<CDir *> petals;
    in->get_dirfrags(petals);
    int brothers_count = 1;
    int brothers_auth_count = 1;
    int bro_dir_count = 1;
    int bro_auth_dir_count = 1;
    for (CDir * petal : petals) {
       brothers_count += petal->get_num_any()+1;
      bro_dir_count +=1;
      if (petal->is_auth()){
        brothers_auth_count += petal->get_num_any()+1;
        bro_auth_dir_count +=1;
    }
    }

    dir->pot_cached.inc(beat_epoch);
    double cached_load = dir->pot_cached.pot_load(beat_epoch, true);
    if (cached_load < 100) {
      return false;
    }
    dir->pot_cached.clear(beat_epoch);
    
    double adj_auth_single = brothers_auth_count ? (cached_load / brothers_auth_count) : 0.0;
    double adj_all_single = brothers_count ? (cached_load / brothers_count) : 0.0;
    double adj_auth_dir = bro_auth_dir_count ? cached_load/bro_auth_dir_count : 0.0;
    double adj_all_dir = bro_dir_count ? cached_load/bro_dir_count : 0.0;
    for (CDir * petal : petals) {
      update_dir_pot_recur(petal, level, 0.5*(1+petal->get_num_any()) * adj_auth_single + 0.5*adj_auth_dir, 0.5*(1+petal->get_num_any()) * adj_all_single + 0.5*adj_all_dir);
    }
    return ret;
  };

  bool update_pot_auth = dir->is_auth();
  //if (!update_pot_auth || !dir->inode->get_parent_dn()) return;
  if (!dir->inode->get_parent_dn()) {
    update_dir_pot(dir);
    return;
  }

  if (update_dir_pot(dir, 1)){
    //if (update_pot_auth)
    //  dir->pot_auth.inc(beat_epoch);
    //dir->pot_all.inc(beat_epoch);
    dir = dir->inode->get_parent_dn()->get_dir();
  }

  while (dir->inode->get_parent_dn()) {
    dir = dir->inode->get_parent_dn()->get_dir();
    // adjust ancestors' pot
    if (update_pot_auth)
      dir->pot_auth.inc(beat_epoch);
    //dir->pot_auth.inc(beat_epoch);
    dir->pot_all.inc(beat_epoch);
  }

  //set<CDir *> authsubs;
  //mds->mdcache->get_auth_subtrees(authsubs);
  //dout(0) << __func__ << " authsubtrees:" << dendl;
  //for (CDir * dir : authsubs) {
  //  string s;
  //  dir->get_inode()->make_path_string(s);
  //  dout(0) << __func__ << "  path: " << s << " pot_auth=" << dir->pot_auth << " pot_all=" << dir->pot_all << dendl;
  //}
}


/*
 * subtract off an exported chunk.
 *  this excludes *dir itself (encode_export_dir should have take care of that)
 *  we _just_ do the parents' nested counters.
 *
 * NOTE: call me _after_ forcing *dir into a subtree root,
 *       but _before_ doing the encode_export_dirs.
 */
void MDBalancer::subtract_export(CDir *dir)
{
  dirfrag_load_vec_t subload = dir->pop_auth_subtree;

  while (true) {
    dir = dir->inode->get_parent_dir();
    if (!dir) break;

    dir->pop_nested.sub(subload);
    dir->pop_auth_subtree_nested.sub(subload);
  }
}


void MDBalancer::add_import(CDir *dir)
{
  dirfrag_load_vec_t subload = dir->pop_auth_subtree;

  while (true) {
    dir = dir->inode->get_parent_dir();
    if (!dir) break;

    dir->pop_nested.add(subload);
    dir->pop_auth_subtree_nested.add(subload);
  }
}

void MDBalancer::adjust_pop_for_rename(CDir *pdir, CDir *dir, bool inc)
{
  bool adjust_subtree_nest = dir->is_auth();
  bool adjust_subtree = adjust_subtree_nest && !dir->is_subtree_root();
  CDir *cur = dir;
  while (true) {
    if (inc) {
      pdir->pop_nested.add(dir->pop_nested);
      if (adjust_subtree) {
	pdir->pop_auth_subtree.add(dir->pop_auth_subtree);
	pdir->pop_lru_subdirs.push_front(&cur->get_inode()->item_pop_lru);
      }

      if (adjust_subtree_nest)
	pdir->pop_auth_subtree_nested.add(dir->pop_auth_subtree_nested);
    } else {
      pdir->pop_nested.sub(dir->pop_nested);
      if (adjust_subtree)
	pdir->pop_auth_subtree.sub(dir->pop_auth_subtree);

      if (adjust_subtree_nest)
	pdir->pop_auth_subtree_nested.sub(dir->pop_auth_subtree_nested);
    }

    if (pdir->is_subtree_root())
      adjust_subtree = false;
    cur = pdir;
    pdir = pdir->inode->get_parent_dir();
    if (!pdir) break;
  }
}

void MDBalancer::handle_mds_failure(mds_rank_t who)
{
  if (0 == who) {
    mds_last_epoch_under_map.clear();
  }
}

int MDBalancer::dump_loads(Formatter *f) const
{
  std::deque<CDir*> dfs;
  if (mds->mdcache->get_root()) {
    mds->mdcache->get_root()->get_dirfrags(dfs);
  } else {
    dout(10) << "no root" << dendl;
  }

  f->open_object_section("loads");

  f->open_array_section("dirfrags");
  while (!dfs.empty()) {
    CDir *dir = dfs.front();
    dfs.pop_front();

    f->open_object_section("dir");
    dir->dump_load(f);
    f->close_section();

    for (auto it = dir->begin(); it != dir->end(); ++it) {
      CInode *in = it->second->get_linkage()->get_inode();
      if (!in || !in->is_dir())
	continue;

      auto&& ls = in->get_dirfrags();
      for (const auto& subdir : ls) {
	if (subdir->pop_nested.meta_load() < .001)
	  continue;
	dfs.push_back(subdir);
      }
    }
  }
  f->close_section();  // dirfrags array

  f->open_object_section("mds_load");
  {

    auto dump_mds_load = [f](mds_load_t& load) {
      f->dump_float("request_rate", load.req_rate);
      f->dump_float("cache_hit_rate", load.cache_hit_rate);
      f->dump_float("queue_length", load.queue_len);
      f->dump_float("cpu_load", load.cpu_load_avg);
      f->dump_float("mds_load", load.mds_load());

      f->open_object_section("auth_dirfrags");
      load.auth.dump(f);
      f->close_section();
      f->open_object_section("all_dirfrags");
      load.all.dump(f);
      f->close_section();
    };

    for (auto p : mds_load) {
      stringstream name;
      name << "mds." << p.first;
      f->open_object_section(name.str().c_str());
      dump_mds_load(p.second);
      f->close_section();
    }
  }
  f->close_section(); // mds_load

  f->open_object_section("mds_meta_load");
  for (auto p : mds_meta_load) {
    stringstream name;
    name << "mds." << p.first;
    f->dump_float(name.str().c_str(), p.second);
  }
  f->close_section(); // mds_meta_load

  f->open_object_section("mds_import_map");
  for (auto p : mds_import_map) {
    stringstream name1;
    name1 << "mds." << p.first;
    f->open_array_section(name1.str().c_str());
    for (auto q : p.second) {
      f->open_object_section("from");
      stringstream name2;
      name2 << "mds." << q.first;
      f->dump_float(name2.str().c_str(), q.second);
      f->close_section();
    }
    f->close_section(); // mds.? array
  }
  f->close_section(); // mds_import_map

  f->close_section(); // loads
  return 0;
}

double MDBalancer::calc_mds_load(mds_load_t load, bool auth)
{ 
  if (!mds->mdcache->root)
  return 0.0;
  double dir_load_level0 = 0.0f;
  set<CDir*> count_candidates;
  mds->mdcache->get_fullauth_subtrees(count_candidates);
  for (set<CDir*>::iterator pot = count_candidates.begin(); pot != count_candidates.end(); ++pot) {
      if ((*pot)->is_freezing() || (*pot)->is_frozen() || (*pot)->get_inode()->is_stray()) continue;
      dir_load_level0 += (*pot)->get_load(this);
  }

  //vector<string> betastrs;
  //pair<double, double> result = req_tracer.alpha_beta("/", total, betastrs);
  pair<double, double> result = mds->mdcache->root->alpha_beta(beat_epoch);
  //double ret = load.mds_load(result.first, result.second, beat_epoch, auth, this);
  double ret = dir_load_level0;
  //dout(0) << __func__ << " load=" << load << " alpha=" << result.first << " beta=" << result.second << " dir_load_level0= " << dir_load_level0 << " pop=" << load.mds_pop_load() << " pot=" << load.mds_pot_load(auth, beat_epoch) << " result=" << ret << dendl;
  //if (result.second < 0) {
  //  dout(7) << __func__ << " Illegal beta detected" << dendl;
  //  for (string s : betastrs) {
  //    dout(7) << __func__ << "   " << s << dendl;
  //  }
  //}
  return ret;
}
