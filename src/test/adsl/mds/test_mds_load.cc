#include "common/debug.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/MDBalancer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "Testing: "

void test_mds_load()
{
	adsl::dirfrag_load_pred_t pl_first;
	adsl::dirfrag_load_pred_t pl_second;
	dout(0) << "pl_first " << pl_first << " pl_second " << pl_second << dendl;

	pl_first.adjust(8.0);
	pl_second.adjust(3.0);
	dout(0) << "pl_first " << pl_first << " pl_second " << pl_second << dendl;

	pl_first.scale(2.0);
	pl_second.scale(0.5);
	dout(0) << "pl_first " << pl_first << " pl_second " << pl_second << dendl;

	pl_first.add(pl_second);
	pl_second.zero();
	dout(0) << "pl_first " << pl_first << " pl_second " << pl_second << dendl;
}

int main(int argc, const char * argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);

	test_mds_load();
}
