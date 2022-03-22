// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __ADSL_STOPWATCHER_H__
#define __ADSL_STOPWATCHER_H__

#include <chrono>

namespace adsl {
namespace util {

using timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
constexpr auto now = std::chrono::high_resolution_clock::now;

class StopWatcher {
  public:
    class Trigger {
	StopWatcher * m_sw;
      public:
	Trigger(StopWatcher * _sw) : m_sw(_sw) {
	  if (m_sw) m_sw->start();
	}
	~Trigger() {
	  if (m_sw) {
	    m_sw->end();
	    m_sw->print();
	  }
	}
    };

    StopWatcher() : m_start(now()) {}
    virtual ~StopWatcher() {}
    static double delta(timepoint start, timepoint end) {
      return std::chrono::duration<double, std::milli>(end-start).count();
    }
    double get_delta() { return delta(m_start, m_end); }
  protected:
    timepoint m_start;
    timepoint m_end;
    virtual void print() = 0;
    void start() { m_start = now(); }
    void end() { m_end = now(); }
};

}; // namespace util
}; // namespace adsl

#endif /* adsl/StopWatcher.h */
