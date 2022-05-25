// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"

#include "TorchPredictor.h"

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
#define dout_prefix *_dout << "mds.predictor[torch] " << __func__ << ' '

#include "MatrixFitter.h"

namespace adsl {

bool TorchPredictor::load_model(string path)
{
  if (last_pred_name == path) {
    predictor_dout(0) << " use already loaded model" << predictor_dendl;
    return true;
  }
  try {
    m_mod = torch::jit::load(path);
  } catch (const c10::Error& e) {
    predictor_dout(0) << " failed to load" << predictor_dendl;
    return false;
  }
  return true;
}

int TorchPredictor::predict(boost::string_view script,
                            vector<LoadArray_Int> &cur_loads,
                            LoadArray_Double &pred_load)
{
  pred_load.clear();
  DataChunk<torch::jit::IValue>::reset_seq();

  using timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
  constexpr auto now = std::chrono::high_resolution_clock::now;

  TorchMatrixFitter fitter(cur_loads);
  DataChunk<torch::jit::IValue> chunk;
  while (fitter.fit(chunk)) {
    std::vector<torch::jit::IValue> inputs;
    inputs.push_back(chunk.m_chunkdata);
    timepoint start_pt = now();
    at::Tensor output = m_mod.forward(inputs).toTensor();
    timepoint end_pt = now();
    double duration = std::chrono::duration<double, std::milli>(end_pt-start_pt).count();
    predictor_dout(15) << "Run: " << duration << "ms" << predictor_dendl;
    fitter.place_result(chunk, output, pred_load);
    predictor_dout(20) << " after place_result, pred_load " << pred_load << predictor_dendl;
  }
  return 0;
}

}; // namespace adsl
