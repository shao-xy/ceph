// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <iostream>
using std::cout;

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "mds/Locker.h"
#include "mds/CInode.h"
#include "mds/MDSRank.h"
#include "mds/Mutation.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
//#undef dout_prefix
//#define dout_prefix *_dout

class Deprecated {
public:
	static void create_mds() {
		Mutex mutex("adsl_test_mds_locks");
		SafeTimer timer(g_ceph_context, mutex);
		Beacon beacon(g_ceph_context, NULL, "adsl_test_mds_locks");
		LogChannelRef nptr_logchannel;
		
		uint64_t nonce = 0;
		get_random_bytes((char*)&nonce, sizeof(nonce));
		std::string public_msgr_type = g_conf->ms_public_type.empty() ? g_conf->get_val<std::string>("ms_type") : g_conf->ms_public_type;
		Messenger *msgr = Messenger::create(g_ceph_context, public_msgr_type,
							  entity_name_t::MDS(-1), "mds",
							  nonce, Messenger::HAS_MANY_CONNECTIONS);
		if (!msgr)
			exit(1);
		MDSMap * mdsmap = NULL;
		MDSRank* mds = new MDSRank(mds_rank_t(0), mutex, nptr_logchannel, 
				timer, beacon, mdsmap, msgr, NULL,
			  NULL, NULL);

		MDRequestRef mdr = mds->mdcache->request_start_internal(CEPH_MDS_OP_EXPORTDIR);
	}
};

void request_cleanup(MDRequestRef& mdr)
{
  //dout(15) << "request_cleanup " << *mdr << dendl;
  cout << "request_cleanup " << *mdr << std::endl;

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
		//dout(0) << "  rdlock: " << rdlock << dendl;
		cout << "  rdlock: " << rdlock << std::endl;
	}
	mdr->rdlocks.clear();
  }
  if (!mdr->wrlocks.empty()) {
  	for (SimpleLock * wrlock : mdr->wrlocks) {
		//dout(0) << "  wrlock: " << wrlock << dendl;
		cout << "  wrlock: " << wrlock << std::endl;
	}
	mdr->wrlocks.clear();
  }
  if (!mdr->xlocks.empty()) {
  	for (SimpleLock * xlock : mdr->xlocks) {
		//dout(0) << "  xlock: " << xlock << dendl;
		cout << "  xlock: " << xlock << std::endl;
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
	
	auto cct = global_init(NULL, args,
	  		 CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_UTILITY,
	  		 0);
	common_init_finish(g_ceph_context);
	// ceph_heap_profiler_init();

	return 0;
	
	MDRequestImpl::Params params;
	params.reqid.name = entity_name_t::MDS((mds_rank_t)7);
	params.reqid.tid = (ceph_tid_t)1234;
	params.initiated = ceph_clock_now();
	params.internal_op = CEPH_MDS_OP_EXPORTDIR;
	// MDRequestRef mdr =
	//	mds->op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params>(params);
	OpTracker op_tracker(g_ceph_context, g_conf->mds_enable_op_tracker,
						 g_conf->osd_num_op_tracker_shard);
	MDRequestRef mdr = op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params>(params);
	//MDRequestRef mdr = new MDRequestImpl(params, NULL);

	//dout(7) << "request_start_internal " << *mdr << " op " << CEPH_MDS_OP_EXPORTDIR << dendl;
	cout << "request_start_internal " << *mdr << " op " << CEPH_MDS_OP_EXPORTDIR << std::endl;

	// mdr->more()->export_dir = dir;
	CInode inode(NULL);

	Locker locker(NULL, NULL);

	set<SimpleLock*> rdlocks;
	set<SimpleLock*> xlocks;
	set<SimpleLock*> wrlocks;
	wrlocks.insert(&inode.filelock);

	bool ret = locker.acquire_locks(mdr, rdlocks, xlocks, wrlocks, NULL, NULL, true);
	//dout(0) << " acquire_locks: " << ret << dendl;
	cout << " acquire_locks: " << ret << std::endl;

	request_cleanup(mdr);
	return 0;
}
