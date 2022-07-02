#include <iostream>
#include <fstream>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/Mantle.h"
#include "mds/adsl/Predictor.h"

using adsl::Predictor;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

void init_data(vector<LoadArray_Int> &loads)
{
	loads.push_back(LoadArray_Int(vector<int>{1,2,3,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,1,3,3,4}));
	loads.push_back(LoadArray_Int(vector<int>{0,2,1,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,7,6,3,5}));
	loads.push_back(LoadArray_Int(vector<int>{3,5,3,1,1}));
}

void test_predictor(const char * fpred)
{
	// init
	Predictor p;
	vector<LoadArray_Int> loads;
	LoadArray_Double pred_load;

	// prepare data
	init_data(loads);

	std::ifstream t(fpred);
	std::stringstream buffer;
	buffer << t.rdbuf();
	p.predict(fpred, buffer.str(), loads, pred_load);

	for (auto it = pred_load.begin();
		 it != pred_load.end();
		 it++) {
		std::cout << *it << ' ';
	}
	std::cout << std::endl;
}

int main(int argc, const char * argv[])
{
	if (argc != 2) {
		std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
		return -1;
	}

	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);
	
	//test_mantle();
	test_predictor(argv[1]);

	return 0;
}
