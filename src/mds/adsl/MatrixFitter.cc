// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"

#include "adsl/util/StopWatcher.h"

// we'll later make it to RADOS object, etc.
#define CURRENT_MODEL_SIZE_HEIGHT_TIME 48
#define CURRENT_MODEL_SIZE_WIDTH_DIR 2258
//#define CURRENT_MODEL_SIZE_WIDTH_DIR 225

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
#define dout_prefix *_dout << "mds.predictor[matfit] " << __func__ << ' '

#include "MatrixFitter.h"

//#define TORCH_DEBUG_BREAKDOWN
//#define TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK

namespace adsl {

#ifdef TORCH_DEBUG_BREAKDOWN
class BreakdownViewer : public util::StopWatcher {
  public:
    BreakdownViewer(string name = "") : name(name) {}
    void set_name(string name) { this->name = name; }
  protected:
    string name;
    void print() override;
};

void BreakdownViewer::print()
{
  predictor_dout(0) << "BREAKDOWN[" << name << "]: " << get_delta() << " ms." << predictor_dendl;
}

BreakdownViewer v;
#ifdef TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK
BreakdownViewer v2;
#endif
#endif

template<> int DataChunk<torch::jit::IValue>::cur_start = 0;

TorchMatrixFitter::TorchMatrixFitter(vector<LoadArray_Int> & raw_loads)
  : MatrixFitter<torch::jit::IValue, at::Tensor>(raw_loads)
{
  raw_rows = raw_loads.size();
  raw_cols = raw_rows > 0 ? raw_loads[0].size() : 0;
  cur_row = 0;
  if (raw_cols > CURRENT_MODEL_SIZE_HEIGHT_TIME) {
    col_start = raw_cols - CURRENT_MODEL_SIZE_HEIGHT_TIME;
    col_width = CURRENT_MODEL_SIZE_HEIGHT_TIME;
  } else {
    col_start = 0;
    col_width = raw_cols;
  }
}

bool TorchMatrixFitter::fit(DataChunk<torch::jit::IValue> & _t_datachunk)
{
  if (!raw_rows || cur_row == raw_rows)   return false;

  int rows_left = raw_rows - cur_row;
  int rows_to_use = (rows_left > CURRENT_MODEL_SIZE_WIDTH_DIR) ? 
                    CURRENT_MODEL_SIZE_WIDTH_DIR : rows_left;

  torch::Tensor chunkdata;
#ifdef TORCH_DEBUG_BREAKDOWN
  //{
  //  v.set_name("Create zero-width chunk");
  //  util::StopWatcher::Trigger t(&v);
#endif
  // Final tensor: fill with data
    //chunkdata = torch::empty({0, CURRENT_MODEL_SIZE_HEIGHT_TIME}, torch::kInt32);
#ifdef TORCH_DEBUG_BREAKDOWN
  //}
  {
    v.set_name("Create chunk");
    util::StopWatcher::Trigger t(&v);
#endif
  vector<torch::Tensor> sub_tensors;
  for (int i = 0; i < rows_to_use; i++) {
#ifdef TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK
    v.set_name("Create chunk:loop " + to_string(i));
    util::StopWatcher::Trigger tr(&v);
#endif
    LoadArray_Int & cur_load_line = m_raw_loads[cur_row + i];
    assert((int)cur_load_line.size() == raw_cols);

    // create data line tensor first
    torch::Tensor t = torch::from_blob(cur_load_line.data() + col_start,
                                       {col_width}, torch::kInt32);
    //predictor_dout(30) << '\n' << t << predictor_dendl;

    // TODO: do we need to clone this tensor?

    // maybe shorter?
    if (col_width < CURRENT_MODEL_SIZE_HEIGHT_TIME) {
      torch::Tensor ex_t = torch::ones({CURRENT_MODEL_SIZE_HEIGHT_TIME - col_width}, torch::kInt32);
      t = torch::cat({ex_t, t}, 0);
    }

#ifdef TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK
    v2.set_name("Create chunk:loop " + to_string(i) + ":push_back");
    util::StopWatcher::Trigger tr2(&v2);
    predictor_dout(0) << "Sub-tensor size: " << t.sizes() << predictor_dendl;
#endif
    sub_tensors.push_back(t);
  }
#ifdef TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK
  {
    v2.set_name("Create chunk:stack");
    util::StopWatcher::Trigger tr2(&v2);
#endif
  chunkdata = torch::stack(sub_tensors, 1);
#ifdef TORCH_DEBUG_BREAKDOWN_CREATE_CHUNK
  }
#endif

  // increase starting anchor
  cur_row += rows_to_use;

  // appned more rows?
  if (rows_to_use < CURRENT_MODEL_SIZE_WIDTH_DIR) {
    torch::Tensor ex_r_t = torch::ones({CURRENT_MODEL_SIZE_HEIGHT_TIME,
					CURRENT_MODEL_SIZE_WIDTH_DIR - rows_to_use},
					torch::kInt32);
    chunkdata = torch::cat({chunkdata, ex_r_t}, 1);
  }
#ifdef TORCH_DEBUG_BREAKDOWN

  }

  {
    //v.set_name("Chunk int2float transpose");
    v.set_name("Chunk int2float");
    util::StopWatcher::Trigger t(&v);
#endif
  // transpose
  //chunkdata = chunkdata.toType(at::kFloat).permute({1, 0});
  chunkdata = chunkdata.toType(at::kFloat); // no need to transpose now: we do this in stack period
  //predictor_dout(10) << "After transpose, shape: " << chunkdata.sizes() << predictor_dendl;
  //predictor_dout(20) << " data:\n" << chunkdata << predictor_dendl;
#ifdef TORCH_DEBUG_BREAKDOWN
  }
#endif

  torch::Tensor max_l;
#ifdef TORCH_DEBUG_BREAKDOWN
  {
    v.set_name("Normalize chunk");
    util::StopWatcher::Trigger t(&v);
#endif
  // normalize
  //std::tuple<torch::Tensor, torch::Tensor> max_classes = torch::max(chunkdata, 0);
  //max_l = std::get<0>(max_classes);
  //max_classes = torch::max(max_l, 0);
  //max_l = std::get<0>(max_classes);
  max_l = torch::ones({1}, torch::kInt32);
  max_l = max_l * 47990;
  chunkdata = chunkdata / max_l;
#ifdef TORCH_DEBUG_BREAKDOWN
  }

  {
    v.set_name("Add dim3 to chunk");
    util::StopWatcher::Trigger t(&v);
#endif
  chunkdata = chunkdata.unsqueeze(0);
#ifdef TORCH_DEBUG_BREAKDOWN
  }
#endif
  predictor_dout(10) << "Final shape: " << chunkdata.sizes() << predictor_dendl;

  // Fill chunk struct
  _t_datachunk.set(chunkdata, rows_to_use, max_l);
  return true;
}

vector<double> TorchMatrixFitter::translate_result(at::Tensor & result, torch::jit::IValue & scale_fac)
{
  at::Tensor final_result;
#ifdef TORCH_DEBUG_BREAKDOWN
  {
    v.set_name("Unnormalize");
    util::StopWatcher::Trigger t(&v);
#endif
  assert(scale_fac.isTensor());
  predictor_dout(30) << "Unscaled result : \n" << result << predictor_dendl;
  final_result = result * scale_fac.toTensor();
  predictor_dout(20) << "Final result : \n" << final_result << predictor_dendl;
#ifdef TORCH_DEBUG_BREAKDOWN
  }
#endif

  at::IntArrayRef shape = final_result.sizes();
  predictor_dout(15) << "Final result shape: " << shape << predictor_dendl;
  assert(shape.size() == 2 && shape[0] == 1);

  vector<double> ret;
#ifdef TORCH_DEBUG_BREAKDOWN
  {
    v.set_name("Chunk2vector");
    util::StopWatcher::Trigger t(&v);
#endif
  float * f_res = (float *) final_result.data_ptr();
  for (int i = 0; i < shape[1]; i++) {
    ret.push_back((double) f_res[i]);
  }
#ifdef TORCH_DEBUG_BREAKDOWN
  }
#endif
  predictor_dout(20) << " ret: " << ret << predictor_dendl;
  return ret;
}

void TorchMatrixFitter::reset()
{
  cur_row = 0;
}

}; // namespace adsl
