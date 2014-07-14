// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

#include <thread>
#include <string>
#include <memory>

#include "interface.h"
#include "buffer_manager.h"
#include "storage_engine.h"
#include "status.h"
#include "common.h"

namespace kdb {

class KingDB: public Interface {
 public:
  KingDB(std::string dbname):
    dbname_(dbname),
    se_(dbname)
  {
  }
  virtual ~KingDB() {}

  virtual Status Get(const std::string& key, Value** value_out) override;
  virtual Status Put(const std::string& key, const std::string& value) override;
  virtual Status PutChunk(const char* key,
                          uint64_t size_key,
                          const char* chunk,
                          uint64_t size_chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value,
                          char * buffer_to_delete);
  virtual Status Remove(const std::string& key) override;

 private:
  std::string dbname_;
  std::mutex mutex_;
  kdb::BufferManager bm_;
  kdb::StorageEngine se_;
};

};

/*
namespace kdb {

class KingDB: public Interface {
 public:
  KingDB() {}
  virtual ~KingDB() {}

  virtual Status Get(const std::string& key, std::string *value_out) override {
    LOG_TRACE("KingDB Get()", "[%s]", key.c_str());
    std::unique_lock<std::mutex> lock(mutex_);
    if (map_.find(key) == map_.end()) {
      return Status::NotFound("not found");
    }
    *value_out = map_[key];
    return Status::OK();
  }

  virtual Status Put(const std::string& key, const std::string& value) override {
    LOG_TRACE("KingDB Put()", "[%s] [%s]", key.c_str(), value.c_str());
    std::unique_lock<std::mutex> lock(mutex_);
    map_[key] = value;
    return Status::OK();
  }

  virtual Status Remove(const std::string& key) override {
    LOG_TRACE("KingDB Remove()", "[%s]", key.c_str());
    std::unique_lock<std::mutex> lock(mutex_);
    if (map_.find(key) == map_.end()) {
      return Status::NotFound("not found");
    }
    map_.erase(key);
    return Status::OK();
  }

 private:
  std::mutex mutex_;
  std::map<std::string, std::string> map_;
};

};

*/


#endif // KINGDB_INTERFACE_MAIN_H_
