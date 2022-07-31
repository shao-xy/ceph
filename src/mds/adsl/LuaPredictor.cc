// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sstream>

#include "mds/mdstypes.h"
//#include "Mantle.h"
#include "LuaPredictor.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_predictor
#define predictor_dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_predictor, lvl)) {\
      subsys = ceph_subsys_mds_predictor;\
    }\
    dout_impl(dout_context, subsys, lvl) dout_prefix

#define predictor_dendl dendl; } while (0)

#undef dout_prefix
#define dout_prefix *_dout << "mds.predictor[lua] "
static int dout_wrapper(lua_State *L)
{
  int level = luaL_checkinteger(L, 1);
  lua_concat(L, lua_gettop(L)-1);
  predictor_dout(level) << lua_tostring(L, 2) << predictor_dendl;
  return 0;
}

#undef dout_prefix
#define dout_prefix *_dout << "mds.predictor "

namespace adsl {

int LuaPredictor::predict(boost::string_view script,
		       vector<LoadArray_Int> &cur_loads,
		       LoadArray_Double &pred_load)
{
  // We assume that this process is fast enough
  Mutex::Locker l(mut);

  // reset stack
  lua_settop(L, 0);

  if (luaL_loadstring(L, script.data())) {
    predictor_dout(0) << "WARNING: could not load predictor: " << lua_tostring(L, -1) << predictor_dendl;
    return -EINVAL;
  }

  // global load matrix
  lua_newtable(L);
  for (size_t i = 0; i < cur_loads.size(); i++) {
    lua_newtable(L);

    LoadArray_Int & load_array = cur_loads[i];
    for (size_t j = 0; j < load_array.size(); j++) {
      lua_pushnumber(L, load_array[j]);
      lua_rawseti(L, -2, j + 1);
    }

    lua_rawseti(L, -2, i + 1);
  }
  //predictor_dout(0) << stack_dump() << predictor_dendl;

  lua_setglobal(L, "load_matrix");
  //predictor_dout(0) << stack_dump() << predictor_dendl;

  // invoke script now
  assert(lua_gettop(L) == 1);
  if (lua_pcall(L, 0, 1, 0) != LUA_OK) {
    predictor_dout(0) << stack_dump() << predictor_dendl;
    predictor_dout(0) << "WARNING: predictor could not execute script: "
		      << lua_tostring(L, -1) << predictor_dendl;
    return -EINVAL;
  }

  //predictor_dout(0) << stack_dump() << predictor_dendl;

  // check if table
  if (!lua_istable(L, -1)) {
    predictor_dout(0) << "WARNING: predictor script returned a malformed response." << predictor_dendl;
    return -EINVAL;
  }

  int last_idx = 0;
  for (lua_pushnil(L); lua_next(L, -2); lua_pop(L, 1)) {
    if (!lua_isnumber(L, -2) || !lua_isnumber(L, -1)) {
      predictor_dout(0) << "WARNING: predictor script returned a malformed response." << predictor_dendl;
      return -EINVAL;
    }
    int cur_idx = lua_tointeger(L, -2);
    double cur_load = lua_tonumber(L, -1);
    if (cur_idx <= last_idx) {
      predictor_dout(0) << "WARNING: predictor script returned an index non-consistent response: " << "(index: " << cur_idx << ", load: " << cur_load << ") expected index: " << (last_idx + 1) << " IGNORED" << predictor_dendl;
      continue;
    } else if (cur_idx == last_idx + 1) {
      pred_load.append(cur_load);
    } else {
      // index is larger
      while (last_idx++ < cur_idx - 1) {
	pred_load.append(0.0);
      }
      pred_load.append(cur_load);
    }
    last_idx = cur_idx;
  }

  return 0;
}

LuaPredictor::LuaPredictor()
  : mut("LuaPredictor")
{
  /* build lua vm state */
  L = luaL_newstate();
  if (!L) {
    predictor_dout(0) << "WARNING: predictor could not load Lua state" << predictor_dendl;
    throw std::bad_alloc();
  }

  /* balancer policies can use basic Lua functions */
  //luaopen_base(L);
  //luaopen_coroutine(L);
  //luaopen_string(L);
  //luaopen_math(L);
  //luaopen_table(L);
  //luaopen_utf8(L);
  luaL_openlibs(L);

  /* setup debugging */
  lua_register(L, "PRED_LOG", dout_wrapper);
}

string LuaPredictor::stack_dump()
{
  std::stringstream ss;
  ss << "Stack dump: ";
  int i, top = lua_gettop(L);
  for (i = 1; i <= top; i++) {
    int t = lua_type(L, i);
    switch (t) {
      case LUA_TSTRING:
        ss << '\'' << lua_tostring(L, i) << '\'';
        break;
      case LUA_TBOOLEAN:
        ss << (lua_toboolean(L, i) ? "true" : "false");
        break;
      case LUA_TNUMBER:
	ss << lua_tonumber(L, i);
        break;
      default:
        ss << lua_typename(L, t);
        break;
    }
    ss << " ";
  }
  return ss.str();
}

};
