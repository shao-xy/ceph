// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_TORCHPREDICTOR_H_
#define _ADSL_METADATA_TORCHPREDICTOR_H_

#include <torch/script.h>
#include "Predictor.h"

namespace adsl {

class TorchPredictor : public PredictorImpl {
  public:
    TorchPredictor() {}
    ~TorchPredictor() {}
    bool load_model(string path);
    int predict(boost::string_view script,
                vector<LoadArray_Int> &cur_loads,
                LoadArray_Double &pred_load) override;

  protected:
    string last_pred_name;
    torch::jit::script::Module m_mod;
};

}; // namespace adsl

#endif /* mds/adsl/TorchPredictor.h */
