#include <ctime>
#include <chrono>

#include <iostream>
#include <list>
using namespace std;

#include "TFContext.h"

#include "tensorflow/c/c_api.h"

#include "TFData.h"

TFContext::TFContext()
	: graph(TF_NewGraph()),
	status(TF_NewStatus()),
	session(NULL),
	session_opts(TF_NewSessionOptions()),
	run_opts(NULL)
{
}

TFContext::~TFContext()
{
    TF_DeleteGraph(graph);
    TF_DeleteSession(session, status);
    TF_DeleteSessionOptions(session_opts);
    TF_DeleteStatus(status);
}

bool TFContext::load_model(const char* saved_model_dir)
{
    const char* tags = "serve"; 
    int ntags = 1;
    session = TF_LoadSessionFromSavedModel(session_opts, run_opts, saved_model_dir, &tags, ntags, graph, NULL, status);
	bool suc = TF_GetCode(status) == TF_OK;
	if (!suc) {
		cout << TF_Message(status) << endl;
	}
	return suc;
}

bool TFContext::predict(TFData & data)
{
	TF_SessionRun(session, NULL,
				  data.input,
				  data.input_vals,
				  data.num_inputs,
				  data.output,
				  data.output_vals,
				  data.num_outputs,
				  NULL, 0, NULL, status);
	return (TF_GetCode(status) == TF_OK);
}

void TFContext::run_test(int round, bool save, const char * input_file, const char * output_file)
{
	// alias
	using timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
	constexpr auto now = std::chrono::high_resolution_clock::now;

	//list<double> durations;
	for (int i = 0; i < round; i++) {
		TFData test_data(this);

		if (save && i == (round - 1)) {
			test_data.save_inputdata(input_file);
		}

		timepoint start = now();

		TF_SessionRun(session, NULL,
					  test_data.input,
					  test_data.input_vals,
					  test_data.num_inputs,
					  test_data.output,
					  test_data.output_vals,
					  test_data.num_outputs,
					  NULL, 0, NULL, status);
		
		timepoint end = now();

		if (TF_GetCode(status) == TF_OK) {
			double duration = std::chrono::duration<double, std::milli>(end-start).count();
			cout << "Run "<< (i+1) << ": " << duration << "ms" << endl;
			//durations.push_back(duration);
		} else {
			cout << "Run "<< (i+1) << ": FAILED. (" << TF_Message(status) << ")" << endl;
			//durations.push_back(-1.0);
		}

		if (save && i == (round - 1)) {
			test_data.save_outputdata(output_file);
		}
	}
}
