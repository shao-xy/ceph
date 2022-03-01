#ifndef _TF_TFCONTEXT_H_
#define _TF_TFCONTEXT_H_

#include <cstdlib>

#include "TFData.h"

class TF_Graph;
class TF_Status;
class TF_Session;
class TF_SessionOptions;
class TF_Buffer;

class TFContext {
	TF_Graph * graph;
	TF_Status * status;
	TF_Session* session;
	TF_SessionOptions * session_opts;
	TF_Buffer * run_opts;

public:
	TFContext();
	~TFContext();

	bool load_model(const char* saved_model_dir);
	bool predict(TFData & data);

	void run_test(int round, bool save = false, const char * input_file = NULL, const char * output_file = NULL);

	friend class TFData;
};

#endif /* tf/TFContext.h */
