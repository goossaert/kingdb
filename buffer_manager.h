// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_BUFFER_MANAGER_H_
#define KINGDB_BUFFER_MANAGER_H_

#include <thread>
#include <map>
#include <array>
#include <string>
#include <vector>
#include "kdb.h"
#include "common.h"

namespace kdb {

class BufferManager {
 public:
  BufferManager() {
    im_live_ = 0;
    im_copy_ = 1;
    num_readers_ = 0;
    can_swap_ = true;    // prevents the double-swapping
    force_swap_ = false; // forces swapping
    buffer_size_ = SIZE_BUFFER_WRITE;
    thread_buffer_handler_ = std::thread(&BufferManager::ProcessingLoop, this);
  }
  ~BufferManager() {}

  Status Get(const std::string& key, Value** value_out);
  Status Put(const std::string& key, const std::string& value);
  Status PutChunk(const char* key,
                  uint64_t size_key,
                  const char* chunk,
                  uint64_t size_chunk,
                  uint64_t offset_chunk,
                  uint64_t size_value,
                  char * buffer_to_delete);
  Status Remove(const std::string& key);

 private:
  Status WriteChunk(const OrderType& op,
                    const char *key,
                    uint64_t size_key,
                    const char *chunk,
                    uint64_t size_chunk,
                    uint64_t offset_chunk,
                    uint64_t size_value,
                    char * buffer_to_delete);
  void ProcessingLoop();

  int im_live_;
  int im_copy_;
  int buffer_size_;
  int num_readers_;
  bool can_swap_;
  bool force_swap_;
  std::array<std::vector<Order>, 2> buffers_;
  std::array<int, 2> sizes_;

  // Using a lock hierarchy to avoid deadlock
  std::mutex mutex_live_write_level1_;
  std::mutex mutex_flush_level2_;
  std::mutex mutex_indices_level3_;
  std::mutex mutex_copy_write_level4_;
  std::mutex mutex_copy_read_level5_;
  std::condition_variable cv_flush_;
  std::condition_variable cv_read_;

  // buffer handler
  std::thread thread_buffer_handler_;
};

};

#endif // KINGDB_BUFFER_MANAGER_H_
