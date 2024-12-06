#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

int main(int argc, const char * argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  dout(0) << "Hello, world!" << dendl;

  return 0;
}
