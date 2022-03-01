// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "common/Mutex.h"

int main(int argc, char * argv[])
{
  Mutex mut("WaitMut");
  std::cout << "\033[1;31mMain thread\'s first lock.\033[0m" << std::endl;
  mut.Lock();
  std::cout << "\033[1;31mMain thread\'s second lock.\033[0m" << std::endl;
  mut.Lock();
  std::cout << "\033[1;31mMain thread\'s first unlock.\033[0m" << std::endl;
  mut.Unlock();
  std::cout << "\033[1;31mMain thread\'s second unlock.\033[0m" << std::endl;
  mut.Unlock();
  std::cout << "\033[1;31mMain thread\'s exit.\033[0m" << std::endl;
  return 0;
}
