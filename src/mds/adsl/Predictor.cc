// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "Predictor.h"
#include "PyPredictor.h"
#include "LuaPredictor.h"

namespace adsl {

bool Predictor::endswith(const string & s, const char * suffix)
{
  size_t ca_len = s.size();
  size_t suf_len = strlen(suffix);
  if (ca_len < suf_len)	return false;

  const char * ca_e = s.c_str() + ca_len;
  const char * ca_p = ca_e - suf_len;
  const char * suf_p = suffix;
  while (ca_p < ca_e) {
    if (*ca_p++ != *suf_p++)	return false;
  }
  return true;
}

Predictor::Predictor()
{
  lua_impl = new LuaPredictor();
  py_impl = new PyPredictor();
}

Predictor::~Predictor()
{
  if (lua_impl)	delete lua_impl;
  if (py_impl)	delete py_impl;

  lua_impl = 0;
  py_impl = 0;
}

int Predictor::predict(string script_name,
		       boost::string_view script,
		       vector<LoadArray_Int> cur_loads,
		       LoadArray_Double &pred_load)
{
  PredictorImpl * impl = 0;

  // switch predictor
  if (endswith(script_name, ".py")) {
    impl = py_impl;
  } else if (endswith(script_name, ".lua")) {
    impl = lua_impl;
  }

  return impl ? impl->predict(script, cur_loads, pred_load) : -EINVAL;
}

}; // namespace adsl
