#include <torch/torch.h>
#include <iostream>

int main() {
	  torch::Tensor tensor = torch::rand({2, 3});
	  std::cout << tensor << std::endl;

	  std::tuple<torch::Tensor, torch::Tensor> max_classes = torch::max(tensor, 0);
	  torch::Tensor max_l = std::get<0>(max_classes);
	  std::cout << "Max tuple: " << max_l << std::endl;

	  torch::Tensor normed = tensor / max_l;
	  std::cout << "Normed tuple: " << normed << std::endl;

	  /*
	  torch::Tensor t2 = torch::ones({1, 2, 3});
	  std::cout << t2 << std::endl;

	  std::cout << t2 / 3.0 << std::endl;

	  std::cout << t2[0][1].data_ptr<float>() << std::endl;
	  */

	  //getchar();
	  //getchar();
	  return 0;
}
