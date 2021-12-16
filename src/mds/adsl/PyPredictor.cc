// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sstream>
#include <boost/python.hpp>

#include "mds/mdstypes.h"
#include "PyPredictor.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_predictor
#undef dout_prefix
#define dout_prefix *_dout << "mds.predictor "
#define predictor_dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_predictor, lvl)) {\
      subsys = ceph_subsys_mds_predictor;\
    }\
    dout_impl(dout_context, subsys, lvl) dout_prefix

#define predictor_dendl dendl; } while (0)

static PyObject* test_func(PyObject * self, PyObject * args)
{
  //return PyInt_FromLong(3L);
  //return boost::python::detail::none();
  Py_RETURN_NONE;
}

static PyObject* dout_wrapper(PyObject * self, PyObject * args)
{
  int debug_lvl;
  const char * msg;
  if (!PyArg_ParseTuple(args, "is", &debug_lvl, &msg)) {
    predictor_dout(0) << "Predictor means to log, but with malformed arguments. " << predictor_dendl;
    return NULL;
  }
  if (debug_lvl > 20)	return NULL;
  if (debug_lvl < 0)	debug_lvl = 0;
  predictor_dout(debug_lvl) << msg << predictor_dendl;
  return boost::python::detail::none();
}

namespace adsl {

PyPredictor::PyPredictor()
{
  // call this function only once, as mentioned in Python-C API
  Py_Initialize();

  // learnt from Python source code
  // builtin functions are unavailable unless '__main__' is imported
  _mod_env_main = PyImport_AddModule("__main__");
  _dict_env_global = PyModule_GetDict(_mod_env_main);

  init_dout_mod();

  _mod_pred = 0;
}

PyPredictor::~PyPredictor()
{
  Py_XDECREF(_mod_pred);
  //Py_DECREF(_mod_dout);
  Py_DECREF(_dict_env_global);
  //Py_DECREF(_mod_env_main);
  // call this function only once, as mentioned in Python-C API
  Py_Finalize();
}

int PyPredictor::predict(boost::string_view script,
			 vector<LoadArray_Int> &cur_loads,
			 LoadArray_Double &pred_load)
{
  PyGILState_STATE state = PyGILState_Ensure();

  int ret;
  PyObject * func = 0;
  PyObject * dict_cur_loads = 0;
  if ((ret = _pre_predict(script, cur_loads, &func, &dict_cur_loads)) < 0) {
    return ret;
  }

  PyObject * result = PyObject_Call(func, dict_cur_loads, NULL);
  if (PyErr_Occurred()) {
    dout_errprint();
    predictor_dout(0) << "fail to execute " << PRED_PYFUNC_NAME << "()" << predictor_dendl;
    _post_predict(func, dict_cur_loads, result);
    return -EINVAL;
  }

  if (!translate_result(result, pred_load)) {
    predictor_dout(0) << "predictor returned a malformed result" << predictor_dendl;
    _post_predict(func, dict_cur_loads, result);
    return -EINVAL;
  }

  _post_predict(func, dict_cur_loads, result);

  PyGILState_Release(state);
  return 0;
}

int PyPredictor::_pre_predict(boost::string_view &script,
			      vector<LoadArray_Int> &cur_loads,
			      PyObject ** p_func,
			      PyObject ** p_pytuple_cur_loads)
{
  // Read method from user defined script and add it into module
  _mod_pred = PyModule_New(PRED_PYMODULE_NAME);
  PyModule_AddStringConstant(_mod_pred, "__file__", "");
  PyObject * dict_locals = PyModule_GetDict(_mod_pred);

  PyObject * result = PyRun_String(script.data(), Py_file_input, _dict_env_global, dict_locals);
  if (!result) {
    predictor_dout(0) << "fail to execute script." << predictor_dendl;
    if (PyErr_Occurred()) {
      dout_errprint();
    }
    _post_predict(dict_locals, result);
    return -EINVAL;
  }
  Py_XDECREF(result);
  (*p_func) = PyObject_GetAttrString(_mod_pred, PRED_PYFUNC_NAME);
  if (!(*p_func) || !PyCallable_Check(*p_func)) {
    predictor_dout(0) << PRED_PYFUNC_NAME << "() method not found in script." << predictor_dendl;
    if (PyErr_Occurred()) {
      dout_errprint();
    }
    _post_predict(*p_func);
    return -EINVAL;
  }

  // Initalize input
  PyObject * pylist_cur_loads = PyList_New(cur_loads.size());
  size_t idx = 0;
  for (auto it = cur_loads.begin();
       it != cur_loads.end();
       it++, idx++) {
    LoadArray_Int & la = *it;
    PyObject * py_load_row = PyList_New(la.size());
    size_t row_len = la.size();
    for (size_t i = 0; i < row_len; i++) {
      PyObject * py_int_load = Py_BuildValue("i", la[i]);
      PyList_SetItem(py_load_row, i, py_int_load);
    }
    PyList_SetItem(pylist_cur_loads, idx, py_load_row);
  }
  (*p_pytuple_cur_loads) = PyTuple_New(Py_ssize_t(1));
  PyTuple_SetItem(*p_pytuple_cur_loads, Py_ssize_t(0), pylist_cur_loads);

  return 0;
}

int PyPredictor::_post_predict(PyObject * func, PyObject * dict_cur_loads, PyObject * result)
{
  Py_XDECREF(func);
  Py_XDECREF(dict_cur_loads);
  //Py_XDECREF(_mod_pred);
  //_mod_pred = 0;
  return 0;
}

void PyPredictor::init_dout_mod()
{
  /*
  PyMethodDef doutfunc_log = {
    DOUT_PYFUNC_NAME_LOG, dout_wrapper, METH_VARARGS,
    "Print debug log back into Ceph."
  };
  PyMethodDef doutfunc_test = {
    "test", test_func, METH_VARARGS,
    "Test function."
  };
  */
  PyMethodDef dout_methods[] = {
    {
      DOUT_PYFUNC_NAME_LOG, dout_wrapper, METH_VARARGS,
      "Print debug log back into Ceph."
    },
    {
      "test", test_func, METH_VARARGS,
      "Test function."
    },
    {NULL, NULL, 0, NULL}
  };
  /*
  struct PyModuleDef dout_def = {
    PyModuleDef_HEAD_INIT,
    DOUT_PYMODULE_NAME,
    "A builtin module to help to print debug log back into Ceph."
    -1,
    dout_methods
  };
  _mod_dout = PyModule_Create(&dout_def);
  */
  //_mod_dout = PyModule_New(DOUT_PYMODULE_NAME);
  //PyModule_AddStringConstant(_mod_dout, "__file__", "");
  //PyObject * doutmod_dict = PyModule_GetDict(_mod_dout);
  //PyObject * dout_func_log = PyCFunction_NewEx(&doutfunc_log, NULL, _mod_dout);
  //PyDict_SetItemString(doutmod_dict, DOUT_PYFUNC_NAME_LOG, dout_func_log);
  //PyObject * dout_func_test = PyCFunction_NewEx(&doutfunc_test, NULL, _mod_dout);
  //PyDict_SetItemString(doutmod_dict, "test", dout_func_test);
  //Py_DECREF(doutmod_dict);
  
  _mod_dout = Py_InitModule(DOUT_PYMODULE_NAME, dout_methods);

  // add to global dict
  PyDict_SetItemString(_dict_env_global, DOUT_PYMODULE_NAME, _mod_dout);
  //PyObject * all_imported = PyImport_GetModuleDict();
  //PyDict_SetItemString(all_imported, DOUT_PYMODULE_NAME, _mod_dout);
  //Py_DECREF(all_imported);
  //PyDict_SetItemString(_dict_env_global, DOUT_PYMODULE_NAME, dout_func_log);
}

void PyPredictor::dout_errprint()
{
  using namespace boost::python;
  PyObject * ptype = nullptr;
  PyObject * pvalue = nullptr;
  PyObject * ptraceback = nullptr;

  // Fetch the exception information. If there was no error ptype will be set
  // to null. The other two values might set to null anyway.
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  if (ptype == nullptr) {
    throw std::runtime_error("A Python error was detected but when we called "
                             "PyErr_Fetch() it returned null indicating that "
                             "there was no error.");
  }

  // Sometimes pvalue is not an instance of ptype. This converts it. It's
  // done lazily for performance reasons.
  PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
  //if (ptraceback != nullptr) {
  //  // Unavailable in old versions of Python.
  //  // Implementation copy from Python source code.
  //  // PyException_SetTraceback(pvalue, ptraceback);
  //  if (!(ptraceback == Py_None || PyTraceBack_Check(ptraceback))) {
  //    PyErr_SetString(PyExc_TypeError,
  //      	      "__traceback__ must be a traceback or None");
  //  } else {
  //    Py_INCREF(ptraceback);
  //    // Py_XSETREF(pvalue, ptraceback);
  //    // PyObject *_py_tmp = _PyObject_CAST(pvalue);
  //    PyObject *_py_tmp = pvalue;
  //    pvalue = ptraceback;
  //    Py_XDECREF(_py_tmp);
  //  }
  //}

  // Get Boost handles to the Python objects so we get an easier API.
  handle<> htype(ptype);
  handle<> hvalue(allow_null(pvalue));
  handle<> htraceback(allow_null(ptraceback));

  // Import the `traceback` module and use it to format the exception.
  object traceback = import("traceback");
  object format_exception = traceback.attr("format_exception");
  object formatted_list = format_exception(htype, hvalue, htraceback);
  //object formatted = str("\n").join(formatted_list);
  //string formatted_str = extract<string>(formatted);

  predictor_dout(0) << "Try list traceback: " << predictor_dendl;
  //predictor_dout(0) << formatted_str << predictor_dendl;
  using pyssize_t = boost::python::ssize_t;
  pyssize_t n = boost::python::len(formatted_list);
  for (pyssize_t i = 0; i < n; i++) {
    string en = extract<string>(formatted_list[i]);
    predictor_dout(0) << "  " << en << predictor_dendl;
  }
}

bool PyPredictor::translate_result(PyObject * result, LoadArray_Double &pred_load)
{
  if (!result) {
    predictor_dout(0) << __func__ <<  " Cannot translate: NULL result" << predictor_dendl;
    return false;
  }
  if (pred_load.size() > 0) {
    predictor_dout(0) << __func__ <<  " Warning: non-empty pred_load array." << predictor_dendl;
    pred_load.clear();
  }
  Py_ssize_t len = PyList_Size(result);
  for (Py_ssize_t idx = Py_ssize_t(0);
       idx < len;
       idx++) {
    PyObject * e = PyList_GetItem(result, idx);
    pred_load.append(PyFloat_AsDouble(e));
    Py_DECREF(e);
  }

  return true;
}

}; // namespace adsl
