#include <dirent.h>
#include <errno.h>
#include <cstring>
#include <sstream>

#include "common/debug.h"
#include "PathUtil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_adsl
#undef dout_prefix
#define dout_prefix *_dout << "adsl::common::" << __func__ << ' '

namespace adsl {
namespace util {

void show_dir(const char * path, int dbg_lvl)
{
	DIR * dir;
	struct dirent * ent;

	if ((dir = opendir(path))) {
		dout(dbg_lvl) << "List entries in " << path << dendl;
		std::stringstream ss;
		while ((ent = readdir(dir))) {
			ss << ' ' << ent->d_name;
		}
		dout(dbg_lvl) << ss.str() << dendl;
	} else {
		dout(dbg_lvl) << "Could not open " << path << ", error: " << errno << " (" << strerror(errno) << ")." << dendl;
	}
}

}; // namespace adsl::util
}; // namespace adsl

