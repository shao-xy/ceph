#include <cstdio>
#include <cstdlib>
#include <random>

#include <sstream>

#include "TFData.h"
#include "TFContext.h"

#define SAFE_FREE(p) \
	do { \
		if (p) free(p); \
		p = 0; \
	} while (0)

vector<vector<float> > read_from_file(const char * file)
{
	vector<vector<float> > data;
	if (!file)	return data;

	FILE * fin = fopen(file, "r");
	if (!fin)	return data;

	char * line = NULL;
	size_t len = 0;
	float num;
	while ((getline(&line, &len, fin)) != -1) {
		// handle line
		vector<float> row;
		std::istringstream iss(line);
		while (iss >> num) {
			row.push_back(num);
		}
		data.push_back(row);
	}
	fclose(fin);
	SAFE_FREE(line);
	return data;
}

TFData::TFData(TFContext * ctx, const char * input_file)
	: ctx(ctx), input_file(input_file),
	input(NULL), output(NULL),
	input_vals(NULL), output_vals(NULL),
	dims(NULL), data(NULL)
{
	init_output(&input, &num_inputs, NUM_INPUTS, "serving_default_input_");
	init_output(&output, &num_outputs, NUM_OUTPUTS, "StatefulPartitionedCall", false);

	if (input_file) {
		vector<vector<float> > matrix = read_from_file(input_file);
		init_data(&dims, &ndims, &data, &ndata, &matrix);
	} else {
		init_data(&dims, &ndims, &data, &ndata);
	}

	init_tensors();
}

TFData::~TFData()
{
	ctx = 0;

	SAFE_FREE(input);
	SAFE_FREE(output);
	SAFE_FREE(input_vals);
	SAFE_FREE(output_vals);
	SAFE_FREE(dims);
	SAFE_FREE(data);
}

bool TFData::init_output(TF_Output ** p_outptr, int * p_num, int size, string op_name_prefix, bool mark_id)
{
	if (!ctx)	return false;

	(*p_num) = size;
	(*p_outptr) = (TF_Output *) malloc(sizeof(TF_Output) * size);
	for (int i = 0; i < size; i++) {
		string graph_op_name = op_name_prefix;
		if (mark_id) {
			graph_op_name += std::to_string(i+1);
		}
		TF_Output element = {TF_GraphOperationByName(ctx->graph, graph_op_name.c_str()), 0};
		if (!element.oper)	return false;
		(*p_outptr)[i] = element;
	}
	return true;
}

void TFData::init_data(int64_t ** p_dims, int * p_ndims, float ** p_data, int * p_ndata, vector<vector<float> > *p_data_from_file)
{
	bool gen_random = (p_data_from_file == NULL) || p_data_from_file->empty() || p_data_from_file->front().empty();

	(*p_ndims) = 3;
	(*p_dims) = (int64_t *) malloc((*p_ndims) * sizeof(int64_t));
	(*p_dims)[0] = 1;
	(*p_dims)[1] = gen_random ? 48 : p_data_from_file->size();
	(*p_dims)[2] = gen_random ? 2258 : p_data_from_file->front().size();

	int64_t total_data_size = 1;
	for (int i = 0; i < (*p_ndims); i++) {
		total_data_size *= (*p_dims)[i];
	}
	(*p_ndata) = total_data_size * sizeof(float);
	(*p_data) = (float *) malloc(*p_ndata);

	if (gen_random) {
		static std::default_random_engine e;
		static std::uniform_real_distribution<> dis(0, 1);
		for (int i = 0; i < total_data_size; i++) {
			//(*p_data)[i] = 0.8;
			(*p_data)[i] = dis(e);
		}
	} else {
		int row = (*p_dims)[1];
		int col = (*p_dims)[2];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				int pos = i * col  + j;
				(*p_data)[pos] = p_data_from_file->at(i)[j];
			}
		}
	}
}

bool TFData::init_tensors()
{
	input_vals = (TF_Tensor **) malloc(num_inputs * sizeof(TF_Tensor *));
	output_vals = (TF_Tensor **) malloc(num_outputs * sizeof(TF_Tensor *));

	TF_Tensor * input_tensor = TF_NewTensor(TF_FLOAT, dims, ndims, data, ndata, &NoOpDeallocator, 0);
	if (!input_tensor) {
		return false;
	}
	input_vals[0] = input_tensor;
	return true;
}

bool TFData::save_inputdata(const char * output_file)
{
	if (!dims || !data)	return false;

	FILE * fout = fopen(output_file, "w");
	if (!fout)	return false;

	int row = dims[1];
	int col = dims[2];

	for (int i = 0; i < row; i++) {
		for (int j = 0; j < col; j++) {
			int pos = i * col + j;
			fprintf(fout, "%.3f ", data[pos]);
		}
		fprintf(fout, "\n");
		fflush(fout);
	}

	fclose(fout);
	return true;
}

bool TFData::save_outputdata(const char * output_file)
{
	if (!ctx)	return false;

	auto output_tensor = output[0];
	int output_ndims = TF_GraphGetTensorNumDims(ctx->graph, output_tensor, ctx->status);
	if (TF_GetCode(ctx->status) != TF_OK) {
		return false;
	}
	if (output_ndims != 2) {
		return false;
	}

	int64_t output_dims[2];
	TF_GraphGetTensorShape(ctx->graph, output_tensor, output_dims, output_ndims, ctx->status);
	if (TF_GetCode(ctx->status) != TF_OK) {
		return false;
	}

	float * output_data = (float*) TF_TensorData(output_vals[0]);
	if (!output_data)	return false;

	FILE * fout = fopen(output_file, "w");
	if (!fout)	return false;

	int row = 1; // why this is -1? => // output_dims[0];
	int col = output_dims[1];
	for (int i = 0; i < row; i++) {
		for (int j = 0; j < col; j++) {
			int pos = i * col + j;
			fprintf(fout, "%.9f ", output_data[pos]);
		}
		fprintf(fout, "\n");
		fflush(fout);
	}

	fclose(fout);
	return true;
}
