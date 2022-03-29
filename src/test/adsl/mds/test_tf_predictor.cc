#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/adsl/Predictor.h"
#include "mds/adsl/tf/TFContext.h"

#define MODEL_DIR "/home/ceph/LSTNet/save/web-trace"
#define ROUND 50

using adsl::Predictor;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

int main(int argc, const char * argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);
	
	//TFContext ctx;
	//ctx.load_model(MODEL_DIR);
	////ctx.run_test(ROUND);
	//ctx.run_test(ROUND, true, "data/input.txt", "data/output.txt");

	Predictor p;
	vector<LoadArray_Int> loads;
	LoadArray_Double pred_load;
	p.predict("/web-trace.tf", "", loads, pred_load);
	return 0;
}
