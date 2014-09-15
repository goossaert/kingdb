// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_STORAGE_ENGINE_H_
#define KINGDB_STORAGE_ENGINE_H_

#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include "kingdb/kdb.h"

namespace kdb {

class StorageEngine {
 public:
  StorageEngine(std::string dbname) {
    LOG_TRACE("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
    thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
    sequence_ = 2; // starting at 2, because 0 is the special offset for 'remove'
    num_readers_ = 0;
  }

  ~StorageEngine() {
    thread_index_.join();
    thread_data_.join();
  }

  void ProcessingLoopData() {
    while(true) {
   
      // Wait for orders to process
      LOG_TRACE("StorageEngine::ProcessingLoop()", "start");
      //LOG_TRACE("SE", "WAIT: flush_buffer");
      std::vector<Order> buffer = EventManager::flush_buffer.Wait();     
      LOG_TRACE("StorageEngine::ProcessingLoop()", "got buffer");

      // Wait for readers to exit
      //LOG_TRACE("SE", "WAIT: write_lock");
      mutex_write_.lock();
      while(true) {
        std::unique_lock<std::mutex> lock_read(mutex_read_);
        if (num_readers_ == 0) break;
        cv_read_.wait(lock_read);
      }

      // Process orders, and create update map for the index
      std::map<std::string, uint64_t> map_index;
      for (auto& order: buffer) {
        auto p = key_to_offset_.find(order.key);
        if (p != key_to_offset_.end()) {
          data_.erase(p->second);
          data_.erase(p->second+1);
          key_to_offset_.erase(order.key);
        }
        if (order.type == OrderType::Put) {
          key_to_offset_[order.key] = sequence_;
          data_[sequence_]   = order.key;
          data_[sequence_+1] = order.value;
          map_index[order.key] = sequence_;
          sequence_ += 2;
        } else { // order.type == OrderType::Remove
          map_index[order.key] = 0;
        }
      }

      // Release lock and handle events
      mutex_write_.unlock();

      // Sleep to simulate latency
      std::chrono::milliseconds delay(1000);
      std::this_thread::sleep_for(delay);

      EventManager::flush_buffer.Done();
      //LOG_TRACE("SE", "WAIT: update_index");
      EventManager::update_index.StartAndBlockUntilDone(map_index);
      LOG_TRACE("StorageEngine::ProcessingLoop()", "done");
    }
  }

  void ProcessingLoopIndex() {
    while(true) {
      LOG_TRACE("StorageEngine::ProcessingLoop()", "start");
      std::map<std::string, uint64_t> buffer = EventManager::update_index.Wait();     
      LOG_TRACE("StorageEngine::ProcessingLoop()", "got buffer");
      //LOG_TRACE("INDEX", "WAIT: loop:mutex_index_");
      mutex_index_.lock();
      for (auto& p: buffer) {
        if (p.second == 0) {
          index_.erase(p.first);
        } else {
          index_[p.first] = p.second; 
        }
      }
      mutex_index_.unlock();
      EventManager::update_index.Done();
      LOG_TRACE("StorageEngine::ProcessingLoop()", "done");
      int temp = 1;
      //LOG_TRACE("INDEX", "WAIT: loop:clear_buffer");
      EventManager::clear_buffer.StartAndBlockUntilDone(temp);
    }
  }

  Status Get(const std::string& key, std::string *value_out) {
    //LOG_TRACE("INDEX", "WAIT: Get()-mutex_index_");
    std::unique_lock<std::mutex> lock(mutex_index_);
    LOG_TRACE("StorageEngine::GetEntry()", "%s", key.c_str());
    auto p = index_.find(key);
    if (p != index_.end()) {
      std::string key_temp;
      GetEntry(index_[key], &key_temp, value_out); 
      LOG_TRACE("StorageEngine::GetEntry()", "key:[%s] key_temp:[%s] - value:[%s]", key.c_str(), key_temp.c_str(), value_out->c_str());
      return Status::OK();
    }
    LOG_TRACE("StorageEngine::GetEntry()", "%s - not found!", key.c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }


  Status GetEntry(uint64_t offset, std::string *key_out, std::string *value_out) {
    LOG_TRACE("StorageEngine::GetEntry()", "start");
    Status s = Status::OK();
    mutex_write_.lock();
    mutex_read_.lock();
    num_readers_ += 1;
    mutex_read_.unlock();
    mutex_write_.unlock();

    auto p = data_.find(offset);
    if (p == data_.end()) {
      LOG_TRACE("StorageEngine::GetEntry()", "not found!");
      s = Status::NotFound("Unable to find the entry in the storage engine");
    } else {
      *key_out = data_[offset];
      *value_out = data_[offset+1];
      LOG_TRACE("StorageEngine::GetEntry()", "%s - found [%s]", key_out->c_str(), value_out->c_str());
    }

    mutex_read_.lock();
    num_readers_ -= 1;
    mutex_read_.unlock();
    cv_read_.notify_one();
    return s;
  }

 private:
  // Data
  std::string dbname_;
  std::map<uint64_t, std::string> data_;
  std::map<std::string, uint64_t> key_to_offset_;
  std::thread thread_data_;
  uint64_t sequence_;
  std::condition_variable cv_read_;
  std::mutex mutex_read_;
  std::mutex mutex_write_;
  int num_readers_;

  // Index
  std::map<std::string, uint64_t> index_;
  std::thread thread_index_;
  std::mutex mutex_index_;
};

};

#endif // KINGDB_STORAGE_ENGINE_H_
