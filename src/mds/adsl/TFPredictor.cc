// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TFPredictor.h"

#include "adsl/util/PathUtil.h"

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
    TFData data;
    ret.push_back(DataChunk(data));
    return ret;
}
  
bool TFPredictor::load_model(string path)
{
  adsl::util::show_dir(path.c_str(), 0);
  return false;

  bool r = ctxt.load_model(path.c_str());
  if (r)	m_model_path = path;
  return r;
}

int TFPredictor::predict(boost::string_view script,
			 vector<LoadArray_Int> &cur_loads,
			 LoadArray_Double &pred_load)
{
  // Testing: force use old prediction to avoid errors
  return -1;

  pred_load.clear();
  vector<DataChunk> chunks = fit_size(cur_loads);
  for (DataChunk & chunk : chunks) {
    if (!ctxt.predict(chunk.m_chunkdata)) {
      return -1;
    }
    //for (int i = chunk.m_pos.first; i < chunk.m_pos.second; i++) {
    //  
    //}
  }
  // TODO: format predicted load
  return 0;
}

}; // namespace adsl
