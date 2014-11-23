// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_STORAGE_ENGINE_H_
#define KINGDB_STORAGE_ENGINE_H_

#include <thread>
#include <vector>
#include <map>
#include "kingdb/kdb.h"

namespace kdb {

class StorageEngine {
 public:
  StorageEngine() {
    thread_ = std::thread(&StorageEngine::ProcessingLoop, this);
  }

  ~StorageEngine() {
    thread_.join();
  }

  void ProcessingLoop() {
    while(true) {
      log::trace("StorageEngine::processing_loop()", "start");
      std::vector<Order> buffer = EventManager::flush_buffer.Wait();     
      log::trace("StorageEngine::processing_loop()", "got buffer");
      mutex_data_.lock();
      for (auto& order: buffer) {
        if (order.type == OrderType::Put) {
          data_[order.key] = order.value;
        } else { // order.type == OrderType::Remove
          data_.erase(order.key);
        }
      }
      mutex_data_.unlock();
      EventManager::flush_buffer.Done();
      log::trace("StorageEngine::processing_loop()", "done");
      //EventManager::update_index.StartAndBlockUntilDone(buffer);
    }
  }

  Status GetEntry(const std::string& key, std::string *value_out) {
    std::unique_lock<std::mutex> lock(mutex_data_);
    log::trace("StorageEngine::GetEntry()", "%s", key.c_str());
    auto p = data_.find(key);
    if (p != data_.end()) {
      *value_out = data_[key];
      log::trace("StorageEngine::GetEntry()", "%s - found [%s]", key.c_str(), value_out->c_str());
      return Status::OK();
    }
    log::trace("StorageEngine::GetEntry()", "%s - not found!", key.c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }

 private:
  std::map<std::string, std::string> data_;
  std::thread thread_;
  std::mutex mutex_data_;
};

};

#endif // KINGDB_STORAGE_ENGINE_H_
