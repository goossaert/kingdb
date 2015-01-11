// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_DUMMY_INTERFACE_H_
#define KINGDB_DUMMY_INTERFACE_H_

#include <thread>
#include <string>

#include "interface/interface.h"
#include "util/status.h"
#include "util/logger.h"

namespace kdb {

class DummyInterface: public Interface {
 public:
  DummyInterface() {}
  virtual ~DummyInterface() {}

  virtual Status Get(const std::string& key, std::string *value_out) override {
    log::trace("KingDB Get()", "[%s]", key.c_str());
    std::unique_lock<std::mutex> lock(mutex_);
    if (map_.find(key) == map_.end()) {
      return Status::NotFound("not found");
    }
    *value_out = map_[key];
    return Status::OK();
  }

  virtual Status Put(const std::string& key, const std::string& value) override {
    log::trace("KingDB Put()", "[%s] [%s]", key.c_str(), value.c_str());
    std::unique_lock<std::mutex> lock(mutex_);
    map_[key] = value;
    return Status::OK();
  }

  virtual Status Delete(const std::string& key) override {
    log::trace("KingDB Delete()", "[%s]", key.c_str());
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

#endif // KINGDB_DUMMY_INTERFACE_H_
