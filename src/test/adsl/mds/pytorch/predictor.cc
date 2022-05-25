#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/adsl/Predictor.h"

#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
//#undef dout_prefix
//#define dout_prefix

using adsl::Predictor;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

void init_data(vector<LoadArray_Int> &loads)
{
	//loads.push_back(LoadArray_Int(vector<int>{1000,2000,3000,4000,5000}));
	//loads.push_back(LoadArray_Int(vector<int>{1,1,3,3,4}));
	//loads.push_back(LoadArray_Int(vector<int>{0,2,1,4,5}));
	//loads.push_back(LoadArray_Int(vector<int>{1,7,6,3,5}));
	//loads.push_back(LoadArray_Int(vector<int>{3,5,3,1,1}));
	loads.push_back(LoadArray_Int(vector<int>(48, 2000)));
}

void init_data_2(vector<LoadArray_Int> &loads)
{
	loads.clear();
	for (int i = 0; i < 2258; i++) {
		loads.push_back(LoadArray_Int(vector<int>(48, 15625)));
	}
}

void test_predictor()
{
	// init
	Predictor p;
	vector<LoadArray_Int> loads;
	LoadArray_Double pred_load;

	// prepare data
	//init_data(loads);
	init_data_2(loads);

	using timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
	constexpr auto now = std::chrono::high_resolution_clock::now;

	// prediction
	for (int i = 0; i < 10; i++) {
		timepoint start_pt = now();
		p.predict("/home/ceph/LSTM/torch_test/mds-web-script.pt", "", loads, pred_load);
		//p.predict("/home/ceph/LSTM/LSTNet/mds-web-script-resize.pt", "", loads, pred_load);
		timepoint end_pt = now();
		double duration = std::chrono::duration<double, std::milli>(end_pt-start_pt).count();
		//cout << "Run "<< (i+1) << ": " << duration << "ms" << std::endl;
		dout(0) << "Run "<< (i+1) << ": " << duration << "ms" << dendl;
	}

	p.predict("/home/ceph/LSTM/torch_test/mds-web-script.pt", "", loads, pred_load);
	//p.predict("/home/ceph/LSTM/LSTNet/mds-web-script-resize.pt", "", loads, pred_load);
	//p.predict("/web-trace/mds-web-script.pt", "", loads, pred_load);

	std::stringstream ss;
	for (auto it = pred_load.begin();
		 it != pred_load.end();
		 it++) {
		ss << *it << ' ';
	}
	dout(0) << "Predicted: " << ss.str() << dendl;
}

int main(int argc, const char * argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);
	
	//test_mantle();
	test_predictor();

	return 0;
}
