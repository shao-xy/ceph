// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_PREDICTOR_H_
#define _ADSL_METADATA_PREDICTOR_H_

#include <vector>
using std::vector;

namespace adsl {

class Predictor {
  public:
    struct LoadArray {
      // TODO
      LoadArray();
      double total();
    };

    LoadArray predict(vector<LoadArray> cur_loads);
};

}; // namespace adsl


#endif // mds/adsl/Predictor.h
