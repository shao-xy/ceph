// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <cstring>
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "SocketPredictor.h"

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
#define dout_prefix *_dout << "mds.predictor[sock] " << __func__ << " "

namespace adsl {

SocketPredictor::SocketPredictor()
  : _fd(-1), _available(false)
{
  // start up with no connection
}

SocketPredictor::~SocketPredictor()
{
  close_connection();
}

int SocketPredictor::set_nonblocking()
{
  if (_fd < 0)	return _fd;

  /*
  int ret;
  */
#ifdef _WIN32
  /*
  unsigned long mode = 1; // non-blocking
  if ((ret = ioctlsocket(_fd, FIONBIO, &mode)) < 0) {
    return ret;
  };
  */
  DWORD timeout = RECV_TIMEOUT;
  return setsockopt(_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
#else
  /*
  int flags = fcntl(_fd, F_GETFL, 0);
  if (flags < 0) return flags;
  flags |= O_NONBLOCK;
  if ((ret = fcntl(_fd, F_SETFL, flags)) < 0) {
    return ret;
  }
  */
  struct timeval tv;
  tv.tv_sec = (RECV_TIMEOUT) / 1000;
  tv.tv_usec = ((RECV_TIMEOUT) % 1000) * 1000;
  return setsockopt(_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
#endif
}

void SocketPredictor::close_connection()
{
  if (_fd > 0) {
    ::close(_fd);
  }
  _available = false;
  _fd = -1;
}

int SocketPredictor::connect(string addr, int port)
{
  if (_fd > 0 || _available) {
    // already connected?
    close_connection();
  }

  if ((_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    return _fd;
  }

  int ret;
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  //if ((ret = inet_pton(AF_INET, addr.c_str(), &server_addr.sin_addr)) < 0) {
  //  return ret;
  //}
  server_addr.sin_addr.s_addr = inet_addr(addr.c_str());

  if ((ret = ::connect(_fd, (struct sockaddr *)&server_addr, sizeof(server_addr))) < 0) {
    return ret;
  }

  // make the socket non-blocking
  if ((ret = set_nonblocking()) < 0) {
    return ret;
  }
  
  // now it's useable
  _available = true;
  return 0;
}

#define CHECKFAIL_SEND(buf, len) \
  do { \
    if (::send(_fd, (buf), (len), 0) < 0) { \
      predictor_dout(0) << "Send data failed: " << strerror(errno) << predictor_dendl; \
      return errno; \
    } \
  } while (0)

#define CHECKFAIL_RECV(buf, len) \
  do { \
    if (::recv(_fd, (buf), (len), 0) < 0) { \
      predictor_dout(0) << "Recv data failed: " << strerror(errno) << predictor_dendl; \
      return errno; \
    } \
  } while (0)

int SocketPredictor::predict(boost::string_view script,
			     vector<LoadArray_Int> &cur_loads,
			     LoadArray_Double &pred_load)
{
  if (cur_loads.size() == 0 || cur_loads[0].size() == 0) {
    predictor_dout(0) << "Load matrix with 0 lines of row or column." << predictor_dendl;
    return -1;
  }

  if (_fd < 0) {
    predictor_dout(0) << "Socket not initialized." << predictor_dendl;
    return _fd;
  }

  if (!_available) {
    predictor_dout(0) << "Socket not ready." << predictor_dendl;
    return -1;
  }

  // send
  int row = cur_loads.size();
  int col = cur_loads[0].size();
  int head[2] = {row, col};
  CHECKFAIL_SEND(&head, sizeof(head));
  for (LoadArray_Int & loadarray : cur_loads) {
    int thiscol = loadarray.size();
    if (thiscol != col) {
      predictor_dout(0) << "Load matrix is not a matrix: " << thiscol << " != " << col << predictor_dendl;

      // dump load array
      predictor_dout(0) << "  Dumping load array ..." << predictor_dendl;
      for (LoadArray_Int & la : cur_loads) {
	predictor_dout(0) << "    " << la.size() << " :" << la << predictor_dendl;
      }
      predictor_dout(0) << "  Dumping load array done" << predictor_dendl;
      return -1;
    }
    CHECKFAIL_SEND(loadarray.nums.data(), thiscol * sizeof(int));
  }

  // wait recv
  int recvbuflen = col * sizeof(double);
  double * recvbuf = (double*) malloc(recvbuflen);
  CHECKFAIL_RECV(recvbuf, recvbuflen);

  vector<double> recved(recvbuf, recvbuf + col);
  pred_load.nums.insert(std::end(pred_load), std::begin(recved), std::end(recved));
  return 0;
}

}; // namespace adsl
