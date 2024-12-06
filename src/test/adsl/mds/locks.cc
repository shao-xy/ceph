// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef ADSL_MDS_LOCKER_DEBUG
#define ADSL_MDS_LOCKER_DEBUG
#endif

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/Locker.h"
#include "mds/CInode.h"
#include "mds/MDSRank.h"
#include "mds/Mutation.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

void request_cleanup(MDRequestRef& mdr)
{
  dout(0) << "request_cleanup " << *mdr << dendl;

  if (mdr->has_more()) {
    if (mdr->more()->is_ambiguous_auth)
      mdr->clear_ambiguous_auth();
  }

  // always SEGMENTATION FAULT because we have no mdcache
  // and the pointer is always NULL
  // locker.drop_locks(mdr.get());
  
  // instead, we manually clear the locks
  if (!mdr->rdlocks.empty()) {
    for (SimpleLock * rdlock : mdr->rdlocks) {
      dout(0) << "  rdlock: " << rdlock << dendl;
    }
    mdr->rdlocks.clear();
  }
  if (!mdr->wrlocks.empty()) {
    for (SimpleLock * wrlock : mdr->wrlocks) {
      dout(0) << "  wrlock: " << (*wrlock) << dendl;
    }
    mdr->wrlocks.clear();
    dout(0) << "  after clear, wrlocks.size = " << mdr->wrlocks.size() << dendl;
  }
  if (!mdr->xlocks.empty()) {
    for (SimpleLock * xlock : mdr->xlocks) {
      dout(0) << "  xlock: " << xlock << dendl;
    }
    mdr->xlocks.clear();
  }

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop stickydirs
  for (set<CInode*>::iterator p = mdr->stickydirs.begin();
       p != mdr->stickydirs.end();
       ++p) 
    (*p)->put_stickydirs();

  // drop cache pins
  mdr->drop_pins();

  // remove from session
  mdr->item_session_request.remove_myself();

  mdr->mark_event("cleaned up request");
}

int main(int argc, const char * argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  MDRequestImpl::Params params;
  params.reqid.name = entity_name_t::MDS((mds_rank_t)7);
  params.reqid.tid = (ceph_tid_t)1234;
  params.initiated = ceph_clock_now();
  params.internal_op = CEPH_MDS_OP_EXPORTDIR;
  // MDRequestRef mdr =
  //  mds->op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params>(params);
  OpTracker op_tracker(g_ceph_context, g_conf->mds_enable_op_tracker, g_conf->osd_num_op_tracker_shard);
  MDRequestRef mdr = op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params>(params);
  //MDRequestRef mdr = new MDRequestImpl(params, NULL);
  
  dout(7) << "request_start_internal " << *mdr << " op " << CEPH_MDS_OP_EXPORTDIR << dendl;
  
  // mdr->more()->export_dir = dir;
  CInode inode(NULL);
  
  Locker locker(NULL, NULL);
  
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;
  set<SimpleLock*> wrlocks;
  SimpleLock & focus = inode.filelock;
  wrlocks.insert(&focus);

  dout(0) << "BEFORE acquire_locks, filelock: " << focus << " can_wrlock: " << focus.can_wrlock(-1) << dendl;
  
  bool ret = locker.acquire_locks(mdr, rdlocks, wrlocks, xlocks, NULL, NULL, true);
  dout(0) << " acquire_locks ret: " << ret << dendl;
  
  dout(0) << "AFTER acquire_locks, filelock: " << focus << " can_wrlock: " << focus.can_wrlock(-1) << dendl;

  int state = focus.get_state();
  dout(0) << "   state == " << focus.get_state_name(state) << dendl;
  dout(0) << "   sm can_wrlock=ANY? " << (focus.get_sm()->states[state].can_wrlock == ANY) << dendl;
  dout(0) << "   sm can_wrlock=AUTH? " << (focus.get_sm()->states[state].can_wrlock == AUTH && inode.is_auth()) << dendl;
  dout(0) << "   num_wrlock = " << focus.more()->num_wrlock << dendl;

  focus.get_wrlock(true);

  // MutationRef mut = new MutationImpl();
  // dout(0) << "  mut: wrlocks size = " << mut->wrlocks.size() << dendl;
  // locker.wrlock_force(&focus, mut);
  
  state = focus.get_state();
  dout(0) << "   state == " << focus.get_state_name(state) << dendl;
  dout(0) << "   sm can_wrlock=ANY? " << (focus.get_sm()->states[state].can_wrlock == ANY) << dendl;
  dout(0) << "   sm can_wrlock=AUTH? " << (focus.get_sm()->states[state].can_wrlock == AUTH && inode.is_auth()) << dendl;
  dout(0) << "   num_wrlock = " << focus.more()->num_wrlock << dendl;

  request_cleanup(mdr);

  return 0;
}
