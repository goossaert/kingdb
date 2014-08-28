// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include <string>
#include <sstream>
#include <vector>
#include <set>
#include <stdlib.h>
#include <random>
#include <chrono>
#include <libmemcached/memcached.hpp>
#include "murmurhash3.h"

#include "status.h"
#include "logger.h"
#include "threadpool.h"
#include "kdb.h"

#define MAX_RETRIES 1

namespace kdb {

class Client {
 public:
  Client(std::string database) {
    memc = memcached(database.c_str(), database.length());
  }
  ~Client() {
    memcached_free(memc);
  }

  uint64_t hash_function(const std::string& key) {
    static char hash[16];
    static uint64_t output;
    MurmurHash3_x64_128(key.c_str(), key.size(), 0, hash);
    memcpy(&output, hash, 8); 
    return output;
  }

  Status Get(const std::string& key, char **value_out, int *size_value) {
    char* buffer = new char[SIZE_BUFFER_CLIENT];
    memcached_return_t rc;
    const char* keys[1];
    keys[0] = key.c_str();
    size_t key_length[]= {key.length()};
    uint32_t flags;

    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_length;
    char *return_value;
    size_t return_value_length;

    rc = memcached_mget(memc, keys, key_length, 1);
    if (rc != MEMCACHED_SUCCESS) {
      std::string msg = key + " " + memcached_strerror(memc, rc);
      return Status::IOError(msg);
    }

    while ((return_value = memcached_fetch(memc,
                                           return_key,
                                           &return_key_length,
                                           &return_value_length,
                                           &flags,
                                           &rc))) {
      memcpy(buffer, return_value, return_value_length);
      buffer[return_value_length] = '\0';
      *value_out = buffer;
      *size_value = return_value_length;
      free(return_value);
    }

    if (rc ==  MEMCACHED_NOTFOUND) {
      return Status::NotFound("key: " + key);
    } else if (rc != MEMCACHED_END) {
      return Status::IOError(key + " " + memcached_strerror(memc, rc));
    }

    return Status::OK(); 
  }


  Status Put(const std::string& key, const std::string& value) {
    memcached_return_t rc = memcached_set(memc, key.c_str(), key.length(), value.c_str(), value.length(), (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
      std::string msg = key + " " + memcached_strerror(memc, rc);
      return Status::IOError(msg);
    }
    return Status::OK();
  }

  Status Put(const char* key, uint64_t size_key, const char *value, uint64_t size_value) {
    memcached_return_t rc = memcached_set(memc, key, size_key, value, size_value, (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
      std::string msg = std::string(key) + " " + memcached_strerror(memc, rc);
      return Status::IOError(msg);
    }
    return Status::OK();
  }

  Status Remove(const char* key, uint64_t size_key) {
    memcached_return_t rc = memcached_delete(memc, key, size_key, (time_t)0);
    if (rc != MEMCACHED_SUCCESS) {
      std::string msg = std::string(key) + " " + memcached_strerror(memc, rc);
      return Status::IOError(msg);
    }
    return Status::OK();
  }


 private:
  memcached_st *memc;
};




class ClientTask: public Task {
 public:
  ClientTask(std::string database, int num_writes, int num_removes, int num_reads) {
    database_ = database;
    num_writes_ = num_writes;
    num_removes_ = num_removes;
    num_reads_ = num_reads;
  }
  virtual ~ClientTask() {};

  virtual void RunInLock(std::thread::id tid) {
    //std::cout << "Thread " << tid << std::endl;
  }

  virtual void Run(std::thread::id tid, uint64_t id) {
    Client client(database_);
    int size = SIZE_LARGE_TEST_ITEMS;
    char *buffer_large = new char[size+1];
    for (auto i = 0; i < size; i++) {
      buffer_large[i] = 'a';
    }
    buffer_large[size] = '\0';

    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    Status s;



    std::seed_seq seq{1, 2, 3, 4, 5, 6, 7};
    std::mt19937 generator(seq);
    std::uniform_int_distribution<int> random_dist(256*1024, 512*1024);

    for (auto i = 0; i < num_writes_; i++) {
      std::stringstream ss;
      ss << id << "-" << i;
      std::string key = ss.str();
      int size_value = random_dist(generator);
      char *value = MakeValue(key, size_value);
      for (auto retry = 0; retry < MAX_RETRIES; retry++) {
        s = client.Put(ss.str().c_str(), ss.str().size(), value, size_value);
        if (s.IsOK()) {
          retry = MAX_RETRIES;
        } else {
          LOG_INFO("ClientTask", "Put() Error for key [%s]: %s", key.c_str(), s.ToString().c_str());
        }
        
        if (retry == MAX_RETRIES) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        LOG_INFO("ClientTask", "retry key: [%s]", key.c_str());
      }
      LOG_INFO("ClientTask", "Put(%s, size:%llu) - [%s]", ss.str().c_str(), size_value, s.ToString().c_str());
      delete[] value;
    }

    std::mt19937 generator_remove(seq);
    std::uniform_int_distribution<int> random_dist_remove(256*1024, 512*1024);
    for (auto i = 0; i < num_removes_; i++) {
      std::stringstream ss;
      ss << id << "-" << i;
      std::string key = ss.str();
      s = client.Remove(key.c_str(), key.size());
      if (!s.IsOK()) {
        LOG_INFO("ClientTask", "Remove() Error for key [%s]: %s", key.c_str(), s.ToString().c_str());
      } else {
        keys_removed.insert(key);
      }
    }

    std::mt19937 generator2(seq);
    std::uniform_int_distribution<int> random_dist2(256*1024, 512*1024);
    for (auto i = 0; i < num_reads_; i++) {
      std::stringstream ss;
      ss << id << "-" << i;
      std::string key = ss.str();
      int size_value = random_dist2(generator2);

      auto it_find = keys_removed.find(key);
      bool has_item = false;
      if (it_find == keys_removed.end()) has_item = true; 

      char *value = nullptr;
      int size_value_get;
      for (auto retry = 0; retry < MAX_RETRIES; retry++) {
        s = client.Get(key, &value, &size_value_get);
        if (!has_item) { 
          if (s.IsNotFound()) {
            LOG_INFO("ClientTask", "Get() OK for removed key [%s]: %s", key.c_str(), s.ToString().c_str());
            retry = MAX_RETRIES;
          } else {
            LOG_INFO("ClientTask", "Get() Error for removed key [%s]: %s", key.c_str(), s.ToString().c_str());
          }
        } else if (!s.IsOK()) {
          LOG_INFO("ClientTask", "Get() Error for key [%s]: %s", key.c_str(), s.ToString().c_str());
        } else {
          if (size_value != size_value_get) {
            LOG_INFO("ClientTask", "Found error in sizes for %s: [%d] [%d]", key.c_str(), size_value, size_value_get); 
          } else {
            LOG_INFO("ClientTask", "Size OK for %s: [%d] [%d]", key.c_str(), size_value, size_value_get); 
            int ret = VerifyValue(key, size_value, value);
            if (ret < 0) {
              LOG_INFO("ClientTask", "Found error in content for key [%s]", key.c_str());
            } else {
              LOG_INFO("ClientTask", "Verified content of key [%s]", key.c_str());
              retry = MAX_RETRIES;
            }
          }
        }
        if (retry == MAX_RETRIES) break;

        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        LOG_INFO("ClientTask", "retry key: [%s]", key.c_str());
      }
      delete[] value;
    }

    std::stringstream ss;
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    ss << "Done in " << duration << " ms";
    LOG_INFO("ClientTask", "%s", ss.str().c_str());
    
    delete[] buffer_large;
  }



  // TODO: make sure that everything works fine even with non-ascii data:
  // use the hashes from MakeValue2() instead of the hashes from MakeValue()
  char* MakeValue(const std::string& key, int size_value) {
    int size_key = key.size();
    char *str = new char[size_value+1];
    str[size_value] = '\0';
    int i = 0;
    for (i = 0; i < size_value / size_key; i++) {
      memcpy(str + i*size_key, key.c_str(), size_key);
    }
    if (size_value % size_key != 0) {
      memcpy(str + i*size_key, key.c_str(), size_value % size_key);
    }
    return str;
  }

  int VerifyValue(const std::string& key, int size_value, const char* value) {
    int size_key = key.size();
    int i = 0;
    bool error = false;
    for (i = 0; i < size_value / size_key; i++) {
      if (memcmp(value + i*size_key, key.c_str(), size_key)) {
        std::string value2(value + i*size_key, size_key);
        printf("diff i:%d size:%d key:[%s], value:[%s]\n", i, size_key, key.c_str(), value2.c_str());
        error = true;
      }
    }
    if (size_value % size_key != 0) {
      if (memcmp(value + i*size_key, key.c_str(), size_value % size_key)) {
        std::string value2(value, size_value % size_key);
        printf("diff remainder size:%d key:[%s], value:[%s]\n", size_value % size_key, key.c_str(), value2.c_str());
        error = true;
      }
    }
    if (error) return -1;
    return 0;
  }







  char* MakeValue2(const std::string& key, int size_value) {
    static char hash[16];
    MurmurHash3_x64_128(key.c_str(), key.size(), 0, hash);
    char *str = new char[size_value+1];
    str[size_value] = '\0';
    int i = 0;
    for (i = 0; i < size_value / 16; i++) {
      memcpy(str + i*16, hash, 16);
    }
    if (size_value % 16 != 0) {
      memcpy(str + i*16, hash, size_value % 16);
    }
    return str;
  }

  int VerifyValue2(const std::string& key, int size_value, const char* value) {
    static char hash[16];
    MurmurHash3_x64_128(key.c_str(), key.size(), 0, hash);
    int i = 0;
    for (i = 0; i < size_value / 16; i++) {
      if (memcmp(value + i*16, hash, 16)) {
        std::string hash2(hash, 16);
        std::string value2(value, size_value);
        printf("diff key:[%s], hash:[%s] value:[%s]\n", key.c_str(), hash2.c_str(), value2.c_str());
        return -1;
      }
    }
    if (size_value % 16 != 0) {
      if (memcmp(value + i*16, hash, size_value % 16)) {
        return -1; 
      }
    }
    return 0;
  }





  std::string database_;
  int num_writes_;
  int num_reads_;
  int num_removes_;
  std::set<std::string> keys_removed;
};

};


void show_usage(char *program_name) {
  printf("Example: %s --host 127.0.0.1:3490 --num-threads 120 --write 10000 --remove 5000 --read 10000\n", program_name);
}


int main(int argc, char **argv) {
  if (argc == 1) {
    show_usage(argv[0]); 
    exit(0);
  }

  if (argc % 2 == 0) {
    std::cerr << "Error: invalid number of arguments" << std::endl; 
    show_usage(argv[0]); 
    exit(-1);
  }

  std::string host("");
  int num_threads = 0;
  int num_writes = 0;
  int num_removes = 0;
  int num_reads = 0;

  if (argc > 2) {
    for (int i = 1; i < argc; i += 2 ) {
      if (strcmp(argv[i], "--host" ) == 0) {
        host = "--SERVER=" + std::string(argv[i+1]);
      } else if (strcmp(argv[i], "--num-threads" ) == 0) {
        num_threads = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--write" ) == 0) {
        num_writes = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--remove" ) == 0) {
        num_removes = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--read" ) == 0) {
        num_reads = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--log-level" ) == 0) {
        if (kdb::Logger::set_current_level(argv[i+1]) < 0 ) {
          fprintf(stderr, "Unknown log level: [%s]\n", argv[i+1]);
          exit(-1); 
        }
      } else {
        fprintf(stderr, "Unknown parameter [%s]\n", argv[i]);
        exit(-1); 
      }
    }
  }

  if (host == "" || num_threads == 0) {
    fprintf(stderr, "Missing arguments\n");
    exit(-1); 
  }

  kdb::ThreadPool tp(num_threads);
  tp.Start();
  for (auto i = 0; i < num_threads; i++ ) {
    tp.AddTask(new kdb::ClientTask(host, num_writes, num_removes, num_reads));
  }
  return 0;


}

int main2(int argc, char **argv) {
  memcache::Memcache client(argv[1]);
  printf("main() start\n");
  //client.set("key", some_vector_of_chars, time_to_live, flags);
  time_t expiry= 0;
  uint32_t flags= 0;
  std::vector<char> value;
  value.push_back('a');
  value.push_back('b');
  std::string key = "mykey";
  client.set(key, value, expiry, flags);
  printf("main() stop\n");
  return 0;
}
