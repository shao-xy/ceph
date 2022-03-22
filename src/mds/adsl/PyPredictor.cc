// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <cstring>
#include <sstream>
#include <boost/python.hpp>
#include <boost/algorithm/string.hpp>

#include "common/Mutex.h"
#include "mds/mdstypes.h"
#include "PyPredictor.h"

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
#define dout_prefix *_dout << "mds.predictor[py] "

#define PYLOGGER_BUF_SIZE 4096
#define PYLOGGER_FLUSH_SIZE (PYLOGGER_BUF_SIZE - 1)

namespace {
  struct PyLogger {
    // Mutex mut;
    char buf[PYLOGGER_BUF_SIZE] = {0};
    PyLogger() {}
    // PyLogger() : mut("pylogger_mut") {}
    ~PyLogger() { flush(); }

    static size_t get_flush_pos(const char * m) {
      if (!m)	return 0;
      size_t pos = 0;
      while (*m && *m++ != '\n') {
	pos++;
      }
      return pos;
    }

    void _flush(const char * direct_flushstr = 0, size_t n = 0) {
      size_t buflen = strlen(buf);
      if (direct_flushstr) {
	size_t bufleft = PYLOGGER_FLUSH_SIZE - buflen;
	if (n > bufleft) {
	  n = bufleft;
	}
	strncpy(buf + buflen, direct_flushstr, n);
	buf[buflen + n] = '\0';
	predictor_dout(0) << buf << predictor_dendl;
	*buf = '\0';
      } else if (buflen > 0) {
	predictor_dout(0) << buf << predictor_dendl;
	*buf = '\0';
      }
    }
    void _write(const char * m) {
      auto buflen = strlen(buf);
      auto bufleft = PYLOGGER_FLUSH_SIZE - buflen;
      auto msglen = strlen(m);
      size_t flush_len = get_flush_pos(m);
      // already has data?
      if (buflen > 0) {
	size_t real_flush_len = MIN(flush_len, bufleft);
	strncpy(buf + buflen, m, real_flush_len);
	buf[buflen + real_flush_len] = '\0';
	m += real_flush_len;
	msglen -= real_flush_len;
	flush_len -= real_flush_len;
	if (*m) {
	  _flush();
	  if (flush_len == 0) {
	    m++;
	    msglen--;
	    flush_len = get_flush_pos(m);
	  }
	}
	else
	  return;
      }
      while (msglen > 0) {
	while (flush_len > PYLOGGER_FLUSH_SIZE) {
	  _flush(m, PYLOGGER_FLUSH_SIZE);
	  m += PYLOGGER_FLUSH_SIZE;
	  flush_len -= PYLOGGER_FLUSH_SIZE;
	  msglen -= PYLOGGER_FLUSH_SIZE;
	}
	if (m[flush_len]) {
	  // new line
	  _flush(m, flush_len);
	  m += flush_len + 1;
	  msglen -= flush_len + 1;
	  flush_len = get_flush_pos(m);
	} else {
	  // write to buffer
	  strcpy(buf, m);
	  return;
	}
      }
    }
    void write(const char * m) {
      //Mutex::Locker l(mut);
      _write(m);
    }
    void flush() {
      //Mutex::Locker l(mut);
      _flush();
    }
  };
  PyLogger pred_logger;

  PyObject* log_write(PyObject*, PyObject* args) {
    char* m = nullptr;
    if (PyArg_ParseTuple(args, "s", &m)) {
      pred_logger.write(m);
    }
    Py_RETURN_NONE;
  }

  PyObject* log_flush(PyObject*, PyObject*){
    pred_logger.flush();
    Py_RETURN_NONE;
  }

  static PyMethodDef log_methods[] = {
    {"write", log_write, METH_VARARGS, "write stdout and stderr"},
    {"flush", log_flush, METH_VARARGS, "flush"},
    {nullptr, nullptr, 0, nullptr}
  };
}

#undef dout_prefix
#define dout_prefix *_dout << "mds.predictor " << __func__ << " "

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
  if (!PyArg_ParseTuple(args, "(is)", &debug_lvl, &msg)) {
    predictor_dout(0) << "Predictor means to log, but with malformed arguments. " << predictor_dendl;
    Py_RETURN_NONE;
  }
  if (debug_lvl > 20)	Py_RETURN_NONE;
  if (debug_lvl < 0)	debug_lvl = 0;
  predictor_dout(debug_lvl) << msg << predictor_dendl;
  Py_RETURN_NONE;
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

#if PY_MAJOR_VERSION >= 3
  struct PyModuleDef stdout_def = {
    PyModuleDef_HEAD_INIT,
    "predictor_logger",
    "Replace Python's stdout and stderr with Ceph's logging stream",
    -1,
    log_methods
  };
  PyObject * py_logger = PyModule_Create(&stdout_def);
  //Py_INCREF(py_logger);
  PySys_SetObject("stderr", py_logger);
  PySys_SetObject("stdout", py_logger);
#else
  PyObject * py_logger = Py_InitModule("ceph_logger", log_methods);
  //Py_INCREF(py_logger);
  PySys_SetObject(const_cast<char*>("stderr"), py_logger);
  PySys_SetObject(const_cast<char*>("stdout"), py_logger);
#endif
  PyRun_SimpleString("import sys");

  //init_dout_mod();

  _mod_pred = nullptr;
}

PyPredictor::~PyPredictor()
{
  Py_XDECREF(_mod_pred);
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

  PyObject * result = PyObject_Call(func, dict_cur_loads, nullptr);
  if (PyErr_Occurred()) {
    //dout_errprint();
    PyErr_Print();
    predictor_dout(0) << "fail to execute " << PRED_PYFUNC_NAME << "()" << predictor_dendl;
    goto fail;
  }

  if (!translate_result(result, pred_load)) {
    predictor_dout(0) << "predictor returned a malformed result" << predictor_dendl;
    goto fail;
  }

  _post_predict(func, dict_cur_loads, result);
  PyGILState_Release(state);
  return 0;

fail:
  _post_predict(func, dict_cur_loads, result);
  PyGILState_Release(state);
  return -EINVAL;
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
      //dout_errprint();
      PyErr_Print();
    }
    _post_predict(dict_locals, result);
    return -EINVAL;
  }
  //Py_XDECREF(dict_locals);
  Py_XDECREF(result);
  (*p_func) = PyObject_GetAttrString(_mod_pred, PRED_PYFUNC_NAME);
  if (!(*p_func) || !PyCallable_Check(*p_func)) {
    predictor_dout(0) << PRED_PYFUNC_NAME << "() method not found in script." << predictor_dendl;
    if (PyErr_Occurred()) {
      //dout_errprint();
      PyErr_Print();
    }
    Py_XDECREF(*p_func);
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
  //PyObject_GC_Del(dict_cur_loads);
  Py_XDECREF(func);
  Py_XDECREF(dict_cur_loads);
  Py_XDECREF(result);

  Py_XDECREF(_mod_pred);
  _mod_pred = nullptr;

  // Manual call GC
  predictor_dout(0) << " Python GC size: " << PyGC_Collect() << predictor_dendl;

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
  //_mod_dout = PyModule_New(DOUT_PYMODULE_NAME);
  //PyModule_AddStringConstant(_mod_dout, "__file__", "");
  //PyObject * doutmod_dict = PyModule_GetDict(_mod_dout);
  //PyObject * dout_func_log = PyCFunction_NewEx(&doutfunc_log, NULL, _mod_dout);
  //PyDict_SetItemString(doutmod_dict, DOUT_PYFUNC_NAME_LOG, dout_func_log);
  //PyObject * dout_func_test = PyCFunction_NewEx(&doutfunc_test, NULL, _mod_dout);
  //PyDict_SetItemString(doutmod_dict, "test", dout_func_test);
  //Py_DECREF(doutmod_dict);

  PyMethodDef dout_methods[] = {
    {
      DOUT_PYFUNC_NAME_LOG, dout_wrapper, METH_VARARGS,
      "Print debug log back into Ceph."
    },
    {
      "test", test_func, METH_VARARGS,
      "Test function."
    },
    {nullptr, nullptr, 0, nullptr}
  };
#if PY_MAJOR_VERSION >= 3
  struct PyModuleDef dout_def = {
    PyModuleDef_HEAD_INIT,
    DOUT_PYMODULE_NAME,
    "A builtin module to help to print debug log back into Ceph.",
    -1,
    dout_methods
  };
  PyObject * _mod_dout = PyModule_Create(&dout_def);
#else
  PyObject * _mod_dout = Py_InitModule(DOUT_PYMODULE_NAME, dout_methods);
#endif
  //PyModule_AddStringConstant(_mod_dout, "__file__", "");

  // add to global dict
  PyDict_SetItemString(_dict_env_global, DOUT_PYMODULE_NAME, _mod_dout);
  //PyObject * all_imported = PyImport_GetModuleDict();
  //PyDict_SetItemString(all_imported, DOUT_PYMODULE_NAME, _mod_dout);
  //Py_DECREF(all_imported);
  //PyDict_SetItemString(_dict_env_global, DOUT_PYMODULE_NAME, dout_func_log);
}

void PyPredictor::dout_errprint()
{
  PyErr_Print();
  /*
  return;

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
#if PY_MAJOR_VERSION >= 3
  if (ptraceback != nullptr) {
    PyException_SetTraceback(pvalue, ptraceback);

    // Unavailable in old versions of Python.
    // Implementation of PyException_SetTraceback -
    //   copied from Python source code.
    // if (!(ptraceback == Py_None || PyTraceBack_Check(ptraceback))) {
    //   PyErr_SetString(PyExc_TypeError,
    //     	      "__traceback__ must be a traceback or None");
    // } else {
    //   Py_INCREF(ptraceback);
    //   // Py_XSETREF(pvalue, ptraceback);
    //   // PyObject *_py_tmp = _PyObject_CAST(pvalue);
    //   PyObject *_py_tmp = pvalue;
    //   pvalue = ptraceback;
    //   Py_XDECREF(_py_tmp);
    // }
  }
#endif

  // Get Boost handles to the Python objects so we get an easier API.
  handle<> htype(ptype);
  handle<> hvalue(allow_null(pvalue));
  handle<> htraceback(allow_null(ptraceback));

  // Import the `traceback` module and use it to format the exception.
  predictor_dout(0) << "Try list traceback: " << predictor_dendl;
  /#*
  {
    object traceback = import("traceback");
    object format_exception = traceback.attr("format_exception");
    object formatted_list = format_exception(htype, hvalue, htraceback);
    //using pyssize_t = boost::python::ssize_t;
    //pyssize_t n = boost::python::len(formatted_list);
    //for (pyssize_t i = 0; i < n; i++) {
    //  string en = extract<string>(formatted_list[i]);
    //  predictor_dout(0) << "  " << en << predictor_dendl;
    //}
    object formatted = str("\n").join(formatted_list);
    string formatted_str = extract<string>(formatted);

    //predictor_dout(0) << formatted_str << predictor_dendl;
    vector<string> formatted_lines;
    boost::split(formatted_lines, formatted_str, boost::is_any_of("\n"), boost::token_compress_on);
    for (string line : formatted_lines) {
      predictor_dout(0) << line << predictor_dendl;
    }
  }
  *#/
  object extype(htype);
  object traceback(htraceback);

  predictor_dout(0) << "Traceback (most recent call last):" << predictor_dendl;

  //Extract line number (top entry of call stack)
  // if you want to extract another levels of call stack
  // also process traceback.attr("tb_next") recurently
  while (!traceback.is_none()) {
    // try {
      long lineno = extract<long> (traceback.attr("tb_lineno"));
      string filename = extract<string>(traceback.attr("tb_frame").attr("f_code").attr("co_filename"));
      string funcname = extract<string>(traceback.attr("tb_frame").attr("f_code").attr("co_name"));
      traceback = traceback.attr("tb_next");
      predictor_dout(0) << "  File \"" << filename << "\", "
			<< "line " << lineno << ", "
			<< "in " << funcname
			<< predictor_dendl;
    //#*
    } catch (error_already_set) {
      predictor_dout(0) << "Error happens while printing traceback." << predictor_dendl;
      dout_errprint();
    }
    *#/
  }
  //Extract error message
  string strErrorMessage = extract<string>(pvalue);
  predictor_dout(0) << strErrorMessage << "\n" << predictor_dendl;
  */
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
