// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_PREDICTOR_H_
#define _ADSL_METADATA_PREDICTOR_H_

#include <boost/utility/string_view.hpp>

#include <lua.hpp>

#include "mdstypes.h"

namespace adsl {

class Predictor {
  public:
    Predictor();
    ~Predictor() { if (L) lua_close(L); }
    int predict(boost::string_view script,
		vector<LoadArray_Int> cur_loads,
		LoadArray_Double &pred_load);

  protected:
    lua_State *L;
    string stack_dump();
};

}; // namespace adsl


#endif // mds/adsl/Predictor.h
