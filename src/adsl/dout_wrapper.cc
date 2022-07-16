// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <torch/script.h>

#include "dout_wrapper.h"
#include "mds/CDir.h"

template<>
std::ostream & operator<< <torch::Tensor>(std::ostream & os, dout_wrapper<torch::Tensor> && wrapped)
{
  return os << wrapped.m_t.sizes();
}

template<>
std::ostream & operator<< <torch::jit::IValue>(std::ostream & os, dout_wrapper<torch::jit::IValue> && wrapped)
{
  const torch::jit::IValue & _v = wrapped.m_t;
  if (_v.isTensor()) {
    os << _v.toTensor().sizes();
  } else {
    os << "[IValue]";
  }
  return os;
}

template<>
std::ostream & operator<< <CDir*>(std::ostream & os, dout_wrapper<CDir*> && wrapped)
{
  const CDir* dir = wrapped.m_t;
  if (dir) {
    return os << "CDir[" << dir << ' ' << dir->dirfrag() << ' ' << dir->get_path() << "/]";
  } else {
    return os << "CDir[NUL]";
  }
}
