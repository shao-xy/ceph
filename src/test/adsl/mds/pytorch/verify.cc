// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdio>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#include "mds/mdstypes.h"
#include "mds/adsl/Predictor.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
//#undef dout_prefix
//#define dout_prefix

#define TRACE_FILE "/home/ceph/LSTM/LSTNet/mds-web-trace.txt"
#define PREDICTED_FILE "predicted.txt"

#define CURRENT_MODEL_SIZE_HEIGHT_TIME 48
#define CURRENT_MODEL_SIZE_WIDTH_DIR 2258

using adsl::Predictor;
using adsl::PredInputLoad;
using adsl::LoadArray_Int;
using adsl::LoadArray_Double;

class LoadInjector {
    FILE * fp;
    char * line;
    size_t len;
    vector<int> split_line(const char * line, size_t len);
  public:
    LoadInjector(string path);
    ~LoadInjector();
    bool readline(vector<LoadArray_Int> & loads);
};

vector<int> LoadInjector::split_line(const char * line, size_t len)
{
  vector<int> ret;
  if (!line)	return ret;

  size_t pos = 0;
  int num = 0;
  bool last_is_digit = false;
  while (pos <= len) {
    char c = line[pos];
    if (c >= '0' && c <= '9') {
      num = num * 10 + (int) (c - '0');
      last_is_digit = true;
    } else {
      if (last_is_digit) {
	//dout(0) << "   TRACE_NUM num = " << num << dendl;
	ret.push_back(num);
	num = 0;
      }
      last_is_digit = false;
    }
    pos++;
  }
  return ret;
}

LoadInjector::LoadInjector(string path)
  : fp(fopen(path.c_str(), "r")), line(NULL), len((size_t)0)
{}

LoadInjector::~LoadInjector()
{
  if (line)	free(line);
  if (fp)	fclose(fp);
}

bool LoadInjector::readline(vector<LoadArray_Int> & loads)
{
  if (line)	free(line);
  if (getline(&line, &len, fp) == -1)	return false;

  vector<int> epoch_load = split_line(line, len);
  //dout(0) << __func__ << " this line has " << epoch_load.size() << " numbers." << dendl;

  /*
  if (epoch_load.size() != loads.size()) {
    free(line);
    line = NULL;
    return false;
  }
  */

  for (size_t i = 0; i < loads.size(); i++) {
    LoadArray_Int & dir_load = loads[i];
    //dout(0) << __func__ << "  epoch_load[" << i << "]=" << epoch_load[i] << dendl;
    dir_load.shift(1, epoch_load[i]);
  }

  free(line);
  line = NULL;

  return true;
}

class LoadRecorder {
    FILE * fp;
  public:
    LoadRecorder(string path)
      : fp(fopen(path.c_str(), "w"))
    {}
    ~LoadRecorder() { if(fp)	fclose(fp); }

    void record(LoadArray_Double & pred_load);
};

void LoadRecorder::record(LoadArray_Double & pred_load)
{
  for (auto it = pred_load.begin();
       it != pred_load.end();
       it++) {
    fprintf(fp, "%.2f,", *it);
  }
  fprintf(fp, "\n");
  fflush(fp);
}

void test_predictor()
{
  LoadInjector inj(TRACE_FILE);
  LoadRecorder rec(PREDICTED_FILE);
  PredInputLoad input_load;
  vector<LoadArray_Int> & loads = input_load.cur_loads;
  LoadArray_Double pred_load;
  Predictor p;

  for (int i = 0; i < CURRENT_MODEL_SIZE_WIDTH_DIR; i++) {
    loads.push_back(LoadArray_Int(RECENT_LOAD_EPOCH_LENGTH));
  }
  
  for (int i = 0; i < CURRENT_MODEL_SIZE_HEIGHT_TIME; i++) {
    inj.readline(loads);
  }

  assert(loads[0].size() == CURRENT_MODEL_SIZE_HEIGHT_TIME);

  /*
  for (int i = 0; i < CURRENT_MODEL_SIZE_WIDTH_DIR; i++) {
    dout(0) << __func__ << "  loads[" << i << "]: " << loads[i] << dendl;
  }
  */

  int cnt = 1;
  do {
    p.predict("/home/ceph/LSTM/torch_test/mds-web-script.pt", "", input_load, pred_load);
    dout(0) << __func__ << "  current:" << cnt++ << dendl;
    rec.record(pred_load);
  } while (inj.readline(loads));
}

int main(int argc, const char * argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  
  test_predictor();
  
  return 0;
}
