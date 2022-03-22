#include "mds/adsl/Predictor.h"
#include "mds/adsl/TorchPredictor.h"
#include "mds/adsl/MatrixFitter.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_predictor

//using adsl::Predictor;
//using adsl::LoadArray_Int;
//using adsl::LoadArray_Double;
//using adsl::TorchMatrixFitter;
using namespace adsl;

void init_data(vector<LoadArray_Int> &loads)
{
	loads.push_back(LoadArray_Int(vector<int>{1,2,3,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,1,3,3,4}));
	loads.push_back(LoadArray_Int(vector<int>{0,2,1,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,7,6,3,5}));
	loads.push_back(LoadArray_Int(vector<int>{3,5,3,1,1}));
}

int main(int argc, const char * argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
	                       CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);

	vector<LoadArray_Int> loads;
	init_data(loads);

	TorchMatrixFitter fitter(loads);
	DataChunk<torch::jit::IValue> chunk;

	while (fitter.fit(chunk)) {
		dout(0) << chunk << dendl;
	}

	return 0;
}
