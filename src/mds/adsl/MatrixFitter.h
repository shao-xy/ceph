// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __ADSL_MDS_MATRIXFITTER_H__
#define __ADSL_MDS_MATRIXFITTER_H__

#include <vector>
using std::vector;

#include <ostream>
#include <torch/script.h>

#include "mds/mdstypes.h"

#include "adsl/dout_wrapper.h"

namespace adsl {

template <typename T>
struct DataChunk {
  static int cur_start;
  static void reset_seq() { cur_start = 0; }

  T m_chunkdata;
  T m_scalefactor;
  std::pair<int, int> m_pos;
  DataChunk() :
    m_pos(std::make_pair<int, int>(std::move(cur_start), std::move(cur_start))) { }

  DataChunk(T data, int width) :
    m_chunkdata(data),
    m_pos(std::make_pair<int, int>(std::move(cur_start), cur_start + width)) {
    cur_start += m_chunkdata.cur_width;
  }

  void set(T chunkdata, int width, T scalefactor = T(1)) {
    m_chunkdata = chunkdata;
    m_scalefactor = scalefactor;
    cur_start = m_pos.second = m_pos.first + width;
  }

  size_t size() { return m_pos.second - m_pos.first; }
};

template <class T>
std::ostream & operator<<(std::ostream & os, const DataChunk<T> & chunk) {
  return os << "DataChunk[ (" << chunk.m_pos.first << ", " << chunk.m_pos.second << ")\n"
	    << dout_wrapper<T>(chunk.m_chunkdata) << '\n' << "]";
}

template <typename T, typename R>
class MatrixFitter {
  protected:
    vector<LoadArray_Int> & m_raw_loads;
    virtual vector<double> translate_result(R & result, T & scale_fac) = 0;
  public:
    MatrixFitter(vector<LoadArray_Int> & raw_loads) : m_raw_loads(raw_loads) {}
    virtual ~MatrixFitter() {}
    virtual bool fit(DataChunk<T> & _t_datachunk) = 0;
    virtual bool place_result(DataChunk<T> & chunk, R & result, LoadArray_Double & pred_load) {
      vector<double> partial_result = translate_result(result, chunk.m_scalefactor);
      //predictor_dout(0) << " pred_load " << pred_load << " chunk " << chunk << " partial_result " << partial_result << predictor_dendl;
      size_t start_pos = chunk.m_pos.first;
      //predictor_dout(0) << " pred_load.size() " << pred_load.size() << " start_pos " << start_pos << predictor_dendl;
      //if (partial_result.size() != chunk.size() || pred_load.size() != start_pos) {
      if (pred_load.size() != start_pos) {
	return false;
      }
      //for (int i = 0;
      //     i != partial_result.size();
      //     i++) {
      //  pred_load[start_pos + i] = partial_result[i];
      //}
      pred_load.nums.insert(pred_load.nums.end(), partial_result.begin(), partial_result.begin() + chunk.size());
      //predictor_dout(0) << " after insert, pred_load " << pred_load << predictor_dendl;
      return true;
    }
    virtual void reset() {}
};

// for PyTorch
class TorchMatrixFitter : public MatrixFitter<torch::jit::IValue, at::Tensor> {
    int raw_rows;
    int raw_cols;
    int cur_row;
    int col_start;
    int col_width;
  public:
    TorchMatrixFitter(vector<LoadArray_Int> & raw_loads);
    bool fit(DataChunk<torch::jit::IValue> & _t_datachunk) override;
    vector<double> translate_result(at::Tensor & result, torch::jit::IValue & scale_fac) override;

    void reset() override;
};

}; // namespace adsl

#endif // mds/adsl/MatrixFitter.h
