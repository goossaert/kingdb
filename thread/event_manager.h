// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_EVENT_MANAGER_H_
#define KINGDB_EVENT_MANAGER_H_

#include <thread>
#include <condition_variable>
#include <vector>
#include <map>
#include "kingdb/kdb.h"

namespace kdb {

template<typename T>
class Event {
 public:
  Event() { has_data = false; }

  void StartAndBlockUntilDone(T& data) {
    std::unique_lock<std::mutex> lock_start(mutex_unique_);
    std::unique_lock<std::mutex> lock(mutex_);
    data_ = data;
    has_data = true;
    cv_ready_.notify_one();
    cv_done_.wait(lock);
  }

  T Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!has_data) {
      cv_ready_.wait(lock);
    }
    return data_;
  }

  void Done() {
    std::unique_lock<std::mutex> lock(mutex_);
    has_data = false;
    cv_done_.notify_one();
  }

 private:
  T data_;
  bool has_data;
  std::mutex mutex_;        // protect the data held in the object
  std::mutex mutex_unique_; // make sure only one thread can enter the Start method
  std::condition_variable cv_ready_;
  std::condition_variable cv_done_;
};


class EventManager {
 public:
  EventManager() {}
  static Event<std::vector<Order>> flush_buffer;
  static Event<std::multimap<uint64_t, uint64_t>> update_index;
  static Event<int> clear_buffer;
};

}

#endif // KINGDB_EVENT_MANAGER_H_
