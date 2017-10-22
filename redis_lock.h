#ifndef __REDIS_LOCK__
#define __REDIS_LOCK__

#include <iostream>
#include <vector>
#include "hiredis/hiredis.h"
#include "engine_common/Log_r.h"
#include <boost/algorithm/string.hpp>

using namespace std;

const static int RANDOM_LENGTH = 15;
const static std::string  kLogGroup="SETTLE_BP";
const static std::string COLON_DELIMITER = ":";

class RLock {
public:
	RLock();
	~RLock();
public:
	int m_alive_time;
	string m_lock_key;
	string m_lock_val;
};

class RedisLock {
public:
	RedisLock();
	virtual ~RedisLock();

public:
	bool Initialize();
	bool Initialize(const string &redis_config_file, const string &log4cpp_config);
	//bool Initialize(const string &log4cpp_config);
	bool AddServer(const string &ip, const int port);
	void SetRetry(const int count, const int delay);//milliseconds
	bool Lock(const string &lock_key, const int ttl, RLock &lock);//milliseconds
	bool Unlock(const RLock &lock);

private:
	bool LockInstance(redisContext *c, const string &lock_key, const string &lock_val, const int ttl);
	void UnlockInstance(redisContext *c, const string &lock_key, const string &lock_val);
	redisReply* RedisCommandArgv(redisContext *c, int argc, const char **inargv);
	string GetUniqueLockId();
private:
	static int m_default_retry_num;
	static float m_clockDrift;
private:
	string m_unlock_script;
	int m_retry_num;
	int m_retry_delay;
	int m_majority_num;
	int m_fd;
	vector<redisContext *> m_redis_servers;
};

#endif
