#include <iostream>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/Mantle.h"
#include "mds/adsl/Predictor.h"

using adsl::Predictor;
using adsl::PredInputLoad;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

string lua_test_code1 = R"lua(
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
)lua";

string py_test_code1 = R"python(
import sys
def predict(load_matrix):
	ret = []
	for load_array in load_matrix:
		ret.append(load_array[-1])
	#dout.log(0, 'Predicted: ' + str(ret))
	#print(dir(dout))
	#print(dout.log)
	print(sys.path)
	print(ret)
	return ret
)python";

string py_test_code2 = R"python(
def predict(load_matrix):
	ret = []
	for load_array in load_matrix:
		ret.append(load_array[0])
	#dout.log(0, 'Predicted: ' + str(ret))
	print(ret)
	return ret
)python";

string py_test_code3 = R"python(
import tensorflow
def predict(load_matrix):
	ret = []
	for load_array in load_matrix:
		#ret.append(load_array[11])
		ret.append(load_array[1])
	#dout.log(0, 'Predicted: ' + str(ret))
	#print(dout.test())
	print(ret)
	return ret
)python";

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
	m.balance(lua_test_code1, whoami, metrics, targets);
}

void init_data(vector<LoadArray_Int> &loads)
{
	loads.push_back(LoadArray_Int(vector<int>{1,2,3,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,1,3,3,4}));
	loads.push_back(LoadArray_Int(vector<int>{0,2,1,4,5}));
	loads.push_back(LoadArray_Int(vector<int>{1,7,6,3,5}));
	loads.push_back(LoadArray_Int(vector<int>{3,5,3,1,1}));
}

void test_predictor()
{
	// init
	Predictor p;
	PredInputLoad loads;
	LoadArray_Double pred_load;

	// prepare data
	init_data(loads.cur_loads);

	//std::cout << p.predict(lua_test_code1, loads, pred_load) << std::endl;
	//p.predict("test.lua", lua_test_code1, loads, pred_load);
	//p.predict("test.py", py_test_code1, loads, pred_load);
	//pred_load.clear();
	//p.predict("test.py", py_test_code2, loads, pred_load);
	//pred_load.clear();
	//p.predict("test.py", py_test_code3, loads, pred_load);
	p.predict("127.0.0.1.sock", py_test_code1, loads, pred_load);

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
