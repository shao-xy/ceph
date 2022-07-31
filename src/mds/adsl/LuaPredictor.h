// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_LUAPREDICTOR_H_
#define _ADSL_METADATA_LUAPREDICTOR_H_

#include <lua.hpp>

#include "Predictor.h"

#include "common/Mutex.h"

namespace adsl {

class LuaPredictor : public PredictorImpl {
  public:
    LuaPredictor();
    ~LuaPredictor() { if (L) lua_close(L); }
    int predict(boost::string_view script,
		vector<LoadArray_Int> &cur_loads,
		LoadArray_Double &pred_load) override;

  protected:
    Mutex mut;
    lua_State *L;
    string stack_dump();
};

}; // namespace adsl


#endif // mds/adsl/LuaPredictor.h
