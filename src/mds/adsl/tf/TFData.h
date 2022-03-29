#ifndef _TF_TFDATA_H_
#define _TF_TFDATA_H_

#include <string>
using std::string;

#include <vector>
using std::vector;

#include "tensorflow/c/c_api.h"

#define NUM_INPUTS 1
#define NUM_OUTPUTS 1

class TFContext;

struct TFData {
	TFContext * ctx;
	const char * input_file;

	int num_inputs;
	int num_outputs;
	TF_Output * input;
    TF_Output * output;
	TF_Tensor ** input_vals;
	TF_Tensor ** output_vals;

	int ndims;
	int64_t * dims;

	int ndata;
	float * data;

	int cur_width = 0;

	TFData(TFContext * ctx = NULL, const char * input_file = NULL);
	~TFData();

	bool init_output(TF_Output ** p_outptr, int * p_num, int size, string op_name_prefix, bool mark_id = true);
	static void init_data(int64_t ** p_dims, int * p_ndims, float ** p_data, int * p_ndata, int * p_cur_width, vector<vector<float> > *p_data_from_file = NULL);
	bool init_tensors();

	static void NoOpDeallocator(void* data, size_t a, void* b) { }

	bool save_inputdata(const char * output_file);
	bool save_outputdata(const char * output_file);
};

#endif /* tf/TFData.h */
