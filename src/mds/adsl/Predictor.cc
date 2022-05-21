// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "Predictor.h"
#include "PyPredictor.h"
#include "LuaPredictor.h"
#include "SocketPredictor.h"
#include "TorchPredictor.h"

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
  sock_impl = new SocketPredictor();
  torch_impl = new TorchPredictor();
}

Predictor::~Predictor()
{
  if (lua_impl)	delete lua_impl;
  if (py_impl)	delete py_impl;
  if (sock_impl)  delete sock_impl;
  if (torch_impl) delete torch_impl;

  lua_impl = 0;
  py_impl = 0;
  sock_impl = 0;
  torch_impl = 0;
}

int Predictor::predict(string script_name,
		       boost::string_view script,
		       PredInputLoad & input_load,
		       LoadArray_Double &pred_load)
{
  PredictorImpl * impl = 0;

  // switch predictor
  if (endswith(script_name, ".py")) {
    impl = py_impl;
  } else if (endswith(script_name, ".lua")) {
    impl = lua_impl;
  } else if (endswith(script_name, ".sock")) {
    impl = sock_impl;
    script_name = script_name.substr(0, script_name.length() - 5);
    size_t semicolon_pos = script_name.find_last_of(':');
    if (semicolon_pos == string::npos) {
      static_cast<SocketPredictor*>(impl)->connect(script_name);
    } else {
      string addr = script_name.substr(0, semicolon_pos);
      string port_str = script_name.substr(semicolon_pos + 1);
      int port;
      try {
	port = std::stoi(port_str);
	static_cast<SocketPredictor*>(impl)->connect(addr, port);
      } catch (...) {
	// invalid port string
	impl = NULL;
      }
    }
  } else if (endswith(script_name, ".pt")) {
    impl = static_cast<TorchPredictor*>(torch_impl)->load_model(script_name) ? torch_impl : NULL;
  }

  return impl ? impl->do_predict(script, input_load, pred_load) : -EINVAL;
}

bool Predictor::need_read_rados(string pred_name)
{
  return endswith(pred_name, ".lua") || endswith(pred_name, ".py");
}

}; // namespace adsl
