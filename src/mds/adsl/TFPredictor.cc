// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <fstream>
#include <sstream>

#include "TFPredictor.h"

#include "adsl/util/PathUtil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_predictor
#define predictor_dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_predictor, lvl)) {\
      subsys = ceph_subsys_mds_predictor;\
    }\
    dout_impl(dout_context, subsys, lvl) dout_prefix

#define predictor_dendl dendl; } while (0)

#undef dout_prefix
#define dout_prefix *_dout << "mds.predictor[tf] "

namespace adsl {

int TFPredictor::DataChunk::cur_start = 0;

TFPredictor::TFPredictor()
{
}

TFPredictor::~TFPredictor()
{
}

vector<TFPredictor::DataChunk> TFPredictor::fit_size(vector<LoadArray_Int> &cur_loads)
{
    vector<DataChunk> ret;
    // TODO: Only for Test
    TFData data(&ctxt);
    ret.push_back(DataChunk(data));
    return ret;
}
  
bool TFPredictor::load_model(string path)
{
  // adsl::util::show_dir(path.c_str(), 0);
  // string fname = path + "/test.txt";
  // predictor_dout(0) << "Starting to read " << fname << predictor_dendl;
  // std::ifstream ifs(fname);
  // string s;
  // while (std::getline(ifs, s)) {
  //   predictor_dout(0) << ' ' << s << predictor_dendl;
  // }
  // return true;
  //return false;

  predictor_dout(0) << __func__ << " start" << predictor_dendl;
  if (m_model_path == path)	return true;

  bool r = ctxt.load_model(path.c_str());
  if (r)	m_model_path = path;
  return r;
}

int TFPredictor::predict(boost::string_view script,
			 vector<LoadArray_Int> &cur_loads,
			 LoadArray_Double &pred_load)
{
  predictor_dout(0) << __func__ << " start" << predictor_dendl;
  //return -1;

  TFData simple_data(&ctxt);
  ctxt.predict(simple_data);
  predictor_dout(0) << __func__ << " after predict (test)." << predictor_dendl;
  //return -1;

  pred_load.clear();
  vector<DataChunk> chunks = fit_size(cur_loads);
  predictor_dout(0) << __func__ << " after fit size" << predictor_dendl;
  for (DataChunk & chunk : chunks) {
    predictor_dout(0) << __func__ << " iterating: chunk[" << chunk.m_pos.first << ',' << chunk.m_pos.second << "]" << predictor_dendl;
    //TFData & data = chunk.m_chunkdata;
    //assert(data.ctx == &ctxt);
    TFData data(&ctxt);
    if (!ctxt.predict(data)) {
      predictor_dout(0) << __func__ << " end: predict fail" << predictor_dendl;
      return -1;
    }
    //chunk.m_chunkdata.save_outputdata("/web-trace/temp.txt");
    //for (int i = chunk.m_pos.first; i < chunk.m_pos.second; i++) {
    //  
    //}
  }

  predictor_dout(0) << __func__ << " end" << predictor_dendl;
  // Testing: force use old prediction to avoid errors
  return -1;

  // TODO: format predicted load
  return 0;
}

}; // namespace adsl
