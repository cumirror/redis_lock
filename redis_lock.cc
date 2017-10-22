#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <fstream>
#include "redis_lock.h"

RLock::RLock():m_alive_time(0),m_lock_key(""),m_lock_val(""){}

RLock::~RLock(){}

RedisLock::RedisLock() {
	Initialize();
}

RedisLock::~RedisLock() {
	close(m_fd);
	for (int i = 0; i < static_cast<int>(m_redis_servers.size()); i++) {
		redisFree(m_redis_servers[i]);
	}
}

float RedisLock::m_clockDrift = 0.01;

bool RedisLock::Initialize() {
	m_unlock_script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	m_retry_num = 3;
	m_retry_delay = 200;
	m_majority_num = 0;
	m_fd = open("/dev/urandom", O_RDONLY);
	if (m_fd == -1) {
		exit(-1);
		return false;
	}
	//Log_r::Init(log4cpp_conf);
	//Log_r::SetProgName(kLogGroup);
	//Log_r::SetModName(kLogGroup);
	return true;
}

bool RedisLock::Initialize(const string &redis_config_file, const string &log4cpp_conf) {
	m_unlock_script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	m_retry_num = 3;
	m_retry_delay = 200;
	m_majority_num = 0;
	m_fd = open("/dev/urandom", O_RDONLY);
	if (m_fd == -1) {
		exit(-1);
		return false;
	}
	Log_r::Init(log4cpp_conf);
	Log_r::SetProgName(kLogGroup);
	Log_r::SetModName(kLogGroup);

	std::string line;
    ifstream in_stream;
    in_stream.open(redis_config_file.c_str());
    std::vector<std::string> address_vector;
    while (in_stream >> line)
    {
        //cout << line << endl;
        address_vector.push_back(line);
    }

    if (address_vector.size() <= 0) {
    	return false;
    }

    for (size_t i = 0; i < address_vector.size(); i++) {
    	std::vector<std::string> info_vector;
    	boost::split(info_vector, address_vector[i], boost::is_any_of(COLON_DELIMITER));
    	if (info_vector.size() != 2) {
    		Log_r::Error("Wrong format of redis lock config file");
    	} else {
    		Log_r::Debug("Redis host: %s, port: %s", info_vector[0].c_str(), info_vector[1].c_str());
    		AddServer(info_vector[0], atoi(info_vector[1].c_str()));
    	}
    }

	return true;
}

bool RedisLock::AddServer(const string &ip, const int port) {
	redisContext *c;
	struct timeval timeout = {1, 500000}; //1.5 seconds
	c = redisConnectWithTimeout(ip.c_str(), port, timeout);
	if (c == NULL || c->err) {
		if (c) {
			redisFree(c);
		} else {
			printf("Connection error: can't allocate redis context\n");
		}
		exit(1);
	}
	m_redis_servers.push_back(c);
	m_majority_num = static_cast<int>(m_redis_servers.size())/2 + 1;
	return true;
}

void RedisLock::SetRetry(const int count, const int delay) {
    m_retry_num = count;
    m_retry_delay = delay;
}

bool RedisLock::Lock(const string &lock_key, const int ttl, RLock &lock) {
	string val = GetUniqueLockId();
	if (val.empty()) {
		return false;
	}
	lock.m_lock_key = lock_key;
	lock.m_lock_val = val;
	printf("Get the unique value is %s\n", val.c_str());
	int trytime = m_retry_num;
	do {
		int n = 0;
		int startTime = (int)time(NULL) * 1000;
		int slen  = static_cast<int>(m_redis_servers.size());
		for (int i = 0; i < slen; ++i) {
			if (LockInstance(m_redis_servers[i], lock_key, val, ttl)) {
				n++;
			}
		}

		int drift = static_cast<int>(ttl * m_clockDrift) + 2;
		int validityTime = ttl - ((int)time(NULL) * 1000 - startTime) - drift;
		printf("The resource validityTime is %d, n is %d, m_majority_num is %d\n",
			   validityTime, n, m_majority_num);
		if (n >= m_majority_num && validityTime > 0) {
			lock.m_alive_time = validityTime;
			return true;
		} else {
			Unlock(lock);
		}

		int delay = rand() % m_retry_delay + static_cast<int>(floor(m_retry_delay / 2));
		usleep(delay * 1000);
		trytime--;
	} while (trytime > 0);
	return false;
}

bool RedisLock::Unlock(const RLock &lock) {
	int slen = static_cast<int>(m_redis_servers.size());
	for (int i = 0; i < slen; ++i) {
		UnlockInstance(m_redis_servers[i], lock.m_lock_key, lock.m_lock_val);
	}
	return true;
}

bool RedisLock::LockInstance(redisContext *c, const string &lock_key, const string &lock_val, const int ttl) {
	redisReply *reply;
	reply = (redisReply*)redisCommand(c, "set %s %s px %d nx", lock_key.c_str(), lock_val.c_str(), ttl);
	if (reply) {
		printf ("Set return %s [null == fail, OK == success]\n", reply->str);
	}
	if (reply && reply->str && 0 == strcmp(reply->str, "OK")) {
		freeReplyObject(reply);
		return true;
	}
	if (reply) {
		freeReplyObject(reply);
	}
	return false;
}

void RedisLock::UnlockInstance(redisContext *c, const string &lock_key, const string &lock_val) {
	redisReply *reply;
	int argc = 5;
	char const *unlockScriptArgv[] = {(char*)"EVAL",
                                m_unlock_script.c_str(),
                                (char*)"1",
                                lock_key.c_str(),
                                lock_val.c_str()};
    reply = RedisCommandArgv(c, argc, unlockScriptArgv);
    if (reply) {
    	freeReplyObject(reply);
    }
}

redisReply * RedisLock::RedisCommandArgv(redisContext *c, int argc, const char **inargv) {
	size_t *argvlen = (size_t*)malloc(argc * sizeof(size_t));
	for (int j = 0; j < argc; ++j) {
		argvlen[j] = strlen(inargv[j]);
		//printf ("str: %s, length: %zu\n", inargv[j], argvlen[j]);
	}
	redisReply *reply = NULL;
	reply = (redisReply*)redisCommandArgv(c, argc, (const char**)inargv, argvlen);
	if (reply) {
		printf("RedisCommandArgv return: %lld\n", reply->integer);
	}
	free(argvlen);
	return reply;
}

string RedisLock::GetUniqueLockId() {
	unsigned char buffer[RANDOM_LENGTH];
	stringstream ss;
	int num = 0;
	if (read(m_fd, buffer, sizeof(buffer)) == sizeof(buffer)) {
		for (int i = 0; i < RANDOM_LENGTH; i++) {
			num = static_cast<char>(buffer[i]);
			char str[3] = {0};
			snprintf(str, 3, "%02X", buffer[i]);
			ss << str;
		}
	} else {
		printf("Error: GetUniqueLockId faile");
	}
	return ss.str();
}





















