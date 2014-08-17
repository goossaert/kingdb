// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_THREADSTORAGE_H_
#define KINGDB_THREADSTORAGE_H_

#include <mutex>
#include <thread>
#include <map>

namespace kdb {

// TODO: Change this for a "thread_local static" -- when LLVM will support it
// TODO: Be careful, because if threads are renewed, the set of thread ids
//       will grow, and as will the "status" map.
// TODO: Templatize this class so it can be used to create whatever type is
//       needed, and replace the unique mutex by an array of mutexes to avoid
//       lock contention -- they would be multiple maps as well.
class ThreadStorage {
 public:
  uint64_t get() {
    std::thread::id id = std::this_thread::get_id();
    std::unique_lock<std::mutex> lock(mutex_);
    return values_[id];
  }

  void put(uint64_t value) {
    std::thread::id id = std::this_thread::get_id();
    std::unique_lock<std::mutex> lock(mutex_);
    values_[id] = value;
  }

  void reset() {
    std::thread::id id = std::this_thread::get_id();
    std::unique_lock<std::mutex> lock(mutex_);
    values_[id] = 0;
  }

 private:
  std::mutex mutex_;
  std::map<std::thread::id, uint64_t> values_;
};

};

#endif // KINGDB_THREADSTORAGE_H_
