// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _ADSL_METADATA_PYPREDICTOR_H_
#define _ADSL_METADATA_PYPREDICTOR_H_

#include "Python.h"
//#include <python3.6m/Python.h>

#include "Predictor.h"

#define DOUT_PYMODULE_NAME "dout"
#define DOUT_PYFUNC_NAME_LOG "log"
#define PRED_PYMODULE_NAME "predictor"
#define PRED_PYFUNC_NAME "predict"

namespace adsl {

class PyPredictor : public PredictorImpl {
  public:
    PyPredictor();
    ~PyPredictor();
    int predict(boost::string_view script,
		vector<LoadArray_Int> &cur_loads,
		LoadArray_Double &pred_load) override;
  private:
    PyObject * _mod_env_main;
    PyObject * _dict_env_global;

    PyObject * _mod_dout;
    PyObject * _mod_pred;

    int _pre_predict(boost::string_view &script,
		     vector<LoadArray_Int> &cur_loads,
		     PyObject ** p_func,
		     PyObject ** p_pytuple_cur_loads);
    int _post_predict(PyObject * func = 0, PyObject * list_cur_loads = 0, PyObject * result = 0);
    void init_dout_mod();
    void dout_errprint();

    bool translate_result(PyObject * result, LoadArray_Double &pred_load);
};

}; // namespace adsl


#endif // mds/adsl/PyPredictor.h
