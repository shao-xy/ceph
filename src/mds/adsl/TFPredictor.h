// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __MDS_ADSL_TFPREDICTOR_H__
#define __MDS_ADSL_TFPREDICTOR_H__

#include "Predictor.h"
#include "tf/TFContext.h"

namespace adsl {

class TFPredictor : public PredictorImpl {
    struct DataChunk {
      static int cur_start;
      static void reset_seq() { cur_start = 0; }

      TFData m_chunkdata;
      pair<int, int> m_pos;
      DataChunk(TFData data) :
	m_chunkdata(data), m_pos(make_pair<int, int>(std::move(cur_start), cur_start + m_chunkdata.cur_width)) {
	cur_start += m_chunkdata.cur_width;
      }
    };
    vector<DataChunk> fit_size(vector<LoadArray_Int> &cur_loads);

    string m_model_path;
    TFContext ctxt;
  public:
    TFPredictor();
    ~TFPredictor();

    bool load_model(string path);

  protected:
    int predict(boost::string_view script,
		vector<LoadArray_Int> &cur_loads,
		LoadArray_Double &pred_load) override;
};

}; // namespace adsl

#endif /* mds/adsl/TFPredictor.h */
