#include "common/Thread.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include <iostream>
#include <unistd.h>
#include <string>

class thread_test : public Thread {
	public:
		Mutex lock;
		Cond condtion;
		bool ready;

		thread_test() : lock("thread_lock") {
			num = 0;
			ready = false;
			create("test_thread");
		}

		int num;

		void *entry() {
			count();
			return NULL;
		}

		void count() {
			sleep(1);
			while (1) {
				lock.Lock();
				condtion.Wait(lock);
				num++;

				std::cout << num << std::endl;
				lock.Unlock();
			}
		}
};

int main() {
	thread_test t;
	for(int i = 1; i < 10; i++) {
		t.lock.Lock();
		sleep(2);
		t.condtion.Signal();
		t.lock.Unlock();
		usleep(200 * 1000);
	}
	t.kill(9);

	return 0;
}
