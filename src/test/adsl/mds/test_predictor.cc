#include <iostream>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/Mantle.h"
#include "mds/adsl/Predictor.h"

using adsl::Predictor;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

string test1 = R"luascript(
function predict()
    prediction = {}
    for entry, entry_load_list in ipairs(load_matrix) do
        -- prediction[entry] = stats.average(entry_load_list)
        prediction[entry] = entry_load_list[#entry_load_list]
    end 
	PRED_LOG(0, "Prediction: " .. table.concat(prediction, ","))
    return prediction
end

return predict()
)luascript";

void test_mantle()
{
	Mantle m;
	mds_rank_t whoami = (mds_rank_t) 0;
	vector<std::map<string, double>> metrics(5);
	std::map<mds_rank_t, double> targets;

	for (mds_rank_t i = mds_rank_t(0);
		 i < mds_rank_t(5);
		 i++) {
		metrics[i] = {{"auth.meta_load", i+1},
					  {"all.meta_load", i+2},
					  {"req_rate", i+3},
					  {"queue_len", i+4},
					  {"cpu_load_avg", i+5}};
	}
	m.balance(test1, whoami, metrics, targets);
}

void test_predictor()
{
	// init
	Predictor p;
	vector<LoadArray_Int> loads;
	LoadArray_Double pred_load;

	// prepare data
	loads.push_back(LoadArray_Int(vector<int>{1,2,3,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,1,3,3,4}));
	loads.push_back(LoadArray_Int(vector<int>{0,2,1,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,7,6,3,5}));
	loads.push_back(LoadArray_Int(vector<int>{3,5,3,1,1}));

	//std::cout << p.predict(test1, loads, pred_load) << std::endl;
	p.predict(test1, loads, pred_load);

	return;

	for (auto it = pred_load.begin();
		 it != pred_load.end();
		 it++) {
		std::cout << *it << ' ';
	}
	std::cout << std::endl;
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
