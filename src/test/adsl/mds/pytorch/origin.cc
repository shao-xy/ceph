#include <torch/script.h> // One-stop header.

#include <iostream>
#include <memory>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

int main(int argc, const char* argv[]) {
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  if (argc != 2) {
    std::cerr << "usage: example-app <path-to-exported-script-module>\n";
    return -1;
  }


  torch::jit::script::Module module;
  try {
    // Deserialize the ScriptModule from a file using torch::jit::load().
    //module = torch::jit::load(argv[1]);
    module = torch::jit::load("/web-trace/mds-web-script.pt");
  }
  catch (const c10::Error& e) {
    std::cerr << "error loading the model\n";
    return -1;
  }

  std::vector<torch::jit::IValue> inputs;
  inputs.push_back(torch::ones({5, 48, 2258}));

  using timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
  constexpr auto now = std::chrono::high_resolution_clock::now;

  for (int i = 0; i < 10000; i++) {
    timepoint start_pt = now();
    at::Tensor output = module.forward(inputs).toTensor();
	//auto output = module.forward(inputs);
    timepoint end_pt = now();
    double duration = std::chrono::duration<double, std::milli>(end_pt-start_pt).count();
    cout << "Run "<< (i+1) << ": " << duration << "ms" << std::endl;
  }

  at::Tensor output = module.forward(inputs).toTensor();
  //std::cout << output.slice(/*dim=*/1, /*start=*/0, /*end=*/5) << '\n';
  std::cout << output << std::endl;

  std::cout << "ok\n";
}
