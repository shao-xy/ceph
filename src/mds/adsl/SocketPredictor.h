// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __MDS_ADSL_SOCKETPREDICTOR_H__
#define __MDS_ADSL_SOCKETPREDICTOR_H__

#include "Predictor.h"

#define DEFAULT_PORT 6787
#define RECV_TIMEOUT 5000 // milliseconds

namespace adsl {

class SocketPredictor : public PredictorImpl {
    int _fd;
    bool _available;
    int set_nonblocking();
    void close_connection();
  public:
    SocketPredictor();
    ~SocketPredictor();

    int connect(string addr, int port = DEFAULT_PORT);

  protected:
    int predict(boost::string_view script,
		      vector<LoadArray_Int> &cur_loads,
		      LoadArray_Double &pred_load) override;
};

}; // namespace adsl

#endif /* mds/adsl/SocketPredictor.h */
