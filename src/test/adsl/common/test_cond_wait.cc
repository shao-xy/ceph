// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "common/Thread.h"
#include "common/Mutex.h"
#include "common/Cond.h"

class WaitingThread : public Thread {
    Mutex & mut;
    Cond & cond;
  public:
    WaitingThread(Mutex & mut, Cond & cond)
      : mut(mut), cond(cond) {}
  protected:
    void * entry() override {
      std::cout << "Waiting thread starts and waits for waking up." << std::endl;
      mut.Lock();
      cond.Wait(mut);
      std::cout << "Waiting thread wakes up and sleep 3s." << std::endl;
      sleep(3);
      std::cout << "Waiting thread gives back control." << std::endl;
      cond.SignalOne();
      mut.Unlock();
      return NULL;
    }
};

int main(int argc, char * argv[])
{
  Mutex mut("WaitMut");
  Cond cond;
  WaitingThread sub_thrd(mut, cond);
  std::cout << "\033[1;31mMain thread starts. (sleep 3s)\033[0m" << std::endl;
  sub_thrd.create("Sub-Thread");

  sleep(3);
  std::cout << "\033[1;31mMain thread signals waiting thread.\033[0m" << std::endl;
  mut.Lock();
  cond.SignalOne();
  std::cout << "\033[1;31mMain thread waits for control.\033[0m" << std::endl;
  cond.Wait(mut);
  std::cout << "\033[1;31mMain thread acquired control.\033[0m" << std::endl;
  sub_thrd.join();
  std::cout << "\033[1;31mWaiting thread joined.\033[0m" << std::endl;
  mut.Unlock();
  return 0;
}
