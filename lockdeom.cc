#include <cstdlib>
#include <unistd.h>
#include "redis_lock.h"

int main(int argc, char** argv) {
	RedisLock *rl = new RedisLock();
	rl->AddServer("10.210.238.101", 6389);
	//rl->AddServer("10.210.238.101", 7379);
	rl->SetRetry(2, 10);
	while(1) {
		RLock my_lock;
		bool flag = rl->Lock("fooo", 60000, my_lock);//milliseconds
		if (flag) {
			printf("Acquire lock success, client:%s, res:%s, vttl:%d\n",
				my_lock.m_lock_val.c_str(), my_lock.m_lock_key.c_str(), my_lock.m_alive_time);
			sleep(12);
			rl->Unlock(my_lock);
			sleep(2);
		} else {
			printf("Acquire failed, name:%s\n", my_lock.m_lock_val.c_str());
			sleep(rand()%3);
		}
	}
	return 0;
}
