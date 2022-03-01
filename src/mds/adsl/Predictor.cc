// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "Predictor.h"
#include "PyPredictor.h"
#include "LuaPredictor.h"
#include "SocketPredictor.h"
#include "TFPredictor.h"

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

bool Predictor::get_sock_addr(string pred_name, string & addr, int & port)
{
  size_t semicolon_pos = pred_name.find_last_of(':');
  if (semicolon_pos == string::npos) {
    addr = pred_name;
    port = -1;
  } else {
    addr = pred_name.substr(0, semicolon_pos);
    try {
      port = std::stoi(pred_name.substr(semicolon_pos + 1));
    } catch (...) {
      // invalid port string
      return false;
    }
  }
  return true;
}

Predictor::Predictor()
{
  lua_impl = new LuaPredictor();
  py_impl = new PyPredictor();
  sock_impl = new SocketPredictor();
}

Predictor::~Predictor()
{
  if (lua_impl)	delete lua_impl;
  if (py_impl)	delete py_impl;
  if (sock_impl)  delete sock_impl;

  lua_impl = 0;
  py_impl = 0;
  sock_impl = 0;
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
  } else if (endswith(script_name, ".tf")) {
    impl = tf_impl;
    if (!static_cast<TFPredictor*>(impl)->load_model(
	  script_name.substr(0, script_name.length() - 3))) {
      impl = NULL;
    }
  } else if (endswith(script_name, ".sock")) {
    impl = sock_impl;

    string addr;
    int port;
    if (get_sock_addr(script_name.substr(0, script_name.length() - 5), addr, port)) {
      if (port == -1) {
	static_cast<SocketPredictor*>(impl)->connect(addr);
      } else {
	static_cast<SocketPredictor*>(impl)->connect(addr, port);
      }
    } else {
      impl = NULL;
    }
  }

  return impl ? impl->predict(script, cur_loads, pred_load) : -EINVAL;
}

bool Predictor::need_read_rados(string pred_name)
{
  return !endswith(pred_name, ".sock") && !endswith(pred_name, ".tf");
}

}; // namespace adsl
