// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_CMINER_PREDICTOR_H_
#define _ADSL_METADATA_CMINER_PREDICTOR_H_

#include "Predictor.h"

namespace adsl {

class CMinerPredictor : public PredictorImpl {
  public:
    CMinerPredictor();
    ~CMinerPredictor();
    int do_predict(boost::string_view script,
		   PredInputLoad &input_load,
		   LoadArray_Double &pred_load) override;
};

}; // namespace adsl

#endif /* mds/adsl/CMinerPredictor.h */
