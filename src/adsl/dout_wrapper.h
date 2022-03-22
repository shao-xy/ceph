#ifndef __ADSL_DOUT_WRAPPER_H__
#define __ADSL_DOUT_WRAPPER_H__

#include <ostream>

template <typename T>
struct dout_wrapper {
	const T & m_t;
	dout_wrapper(const T & t) : m_t(t) {}
};

template <typename T>
std::ostream & operator<<(std::ostream & os, dout_wrapper<T> && wrapped);

#endif /* adsl/dout_wrapper.h */
