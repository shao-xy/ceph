// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_PREDICTOR_H_
#define _ADSL_METADATA_PREDICTOR_H_

#include <boost/utility/string_view.hpp>

#include "mds/mdstypes.h"

namespace adsl {

class PredictorImpl;

class Predictor {
  PredictorImpl * lua_impl;
  PredictorImpl * py_impl;
  PredictorImpl * sock_impl;
  PredictorImpl * torch_impl;
  static bool endswith(const string & s, const char * suffix);
public:
  Predictor();
  ~Predictor();
  int predict(string script_name,
	      boost::string_view script,
	      vector<LoadArray_Int> cur_loads,
	      LoadArray_Double &pred_load);
  static bool need_read_rados(string pred_name);
};

class PredictorImpl {
  friend class Predictor;
public:
  PredictorImpl() {}
  virtual ~PredictorImpl() {}
protected:
  virtual int predict(boost::string_view script,
		      vector<LoadArray_Int> &cur_loads,
		      LoadArray_Double &pred_load) = 0;
};

}; // namespace adsl

#endif /* mds/adsl/Predictor.h */
