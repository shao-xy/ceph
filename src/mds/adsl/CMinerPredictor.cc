// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CMinerPredictor.h"

namespace adsl {

CMinerPredictor::CMinerPredictor()
{}

CMinerPredictor::~CMinerPredictor()
{}

int do_predict(boost::string_view script,
	       PredInputLoad &input_load,
	       LoadArray_Double &pred_load)
{
  pred_load.clear();
  // TODO
  return 0;
}

}; // namespace adsl
