// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_WRITE_BUFFER_H_
#define KINGDB_WRITE_BUFFER_H_

#include "util/debug.h"
#include <cinttypes>
#include <thread>
#include <map>
#include <array>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>

#include "util/options.h"
#include "util/byte_array.h"
#include "util/order.h"
#include "cache/rate_limiter.h"
#include "thread/event_manager.h"

namespace kdb {

class WriteBuffer {
 public:
  WriteBuffer(const DatabaseOptions& db_options,
              EventManager *event_manager)
      : db_options_(db_options),
        event_manager_(event_manager),
        rate_limiter_(db_options.rate_limit_incoming) {
    stop_requested_ = false;
    im_live_ = 0;
    im_copy_ = 1;
    sizes_[im_live_] = 0;
    sizes_[im_copy_] = 0;
    num_readers_ = 0;
    buffer_size_ = db_options_.write_buffer__size / 2;
    thread_buffer_handler_ = std::thread(&WriteBuffer::ProcessingLoop, this);
    is_closed_ = false;

    log::debug("WriteBuffer::ctor()", "WriteBuffer::ctor() %" PRIu64 " - %s\n", db_options_.rate_limit_incoming, db_options_.write_buffer__mode_str.c_str());
    if (db_options_.write_buffer__mode == kWriteBufferModeAdaptive) {
      log::debug("WriteBuffer::ctor()", "WriteBuffer::ctor() write buffer mode adaptive\n");
    } else {
      log::debug("WriteBuffer::ctor()", "WriteBuffer::ctor() write buffer mode direct\n");
    }

  }
  ~WriteBuffer() { Close(); }
  Status Get(ReadOptions& read_options, ByteArray& key, ByteArray* value_out);
  Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& chunk);
  Status PutPart(WriteOptions& write_options,
                  ByteArray& key,
                  ByteArray& chunk,
                  uint64_t offset_chunk,
                  uint64_t size_value,
                  uint64_t size_value_compressed,
                  uint32_t crc32);
  Status Delete(WriteOptions& write_options, ByteArray& key);
  void Flush();

  void Close () {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    is_closed_ = true;
    Stop();
    Flush();
    cv_flush_.notify_one();
    thread_buffer_handler_.join();
  }

  bool UseRateLimiter() {
    if (   db_options_.write_buffer__mode == kWriteBufferModeAdaptive
        || db_options_.rate_limit_incoming > 0) {
      return true;
    }
    return false;
  }

  bool IsStopRequestedAndBufferEmpty() {
    return (   IsStopRequested()
            && buffers_[im_live_].empty()
            && buffers_[im_copy_].empty());
  }
  bool IsStopRequested() { return stop_requested_; }
  void Stop() { stop_requested_ = true; }
  bool stop_requested_;

 private:
  Status WritePart(const WriteOptions& write_options,
                    const OrderType& op,
                    ByteArray& key,
                    ByteArray& chunk,
                    uint64_t offset_chunk,
                    uint64_t size_value,
                    uint64_t size_value_compressed,
                    uint32_t crc32);
  void ProcessingLoop();

  DatabaseOptions db_options_;
  int im_live_;
  int im_copy_;
  uint64_t buffer_size_;
  int num_readers_;
  std::array<std::vector<Order>, 2> buffers_;
  std::array<int, 2> sizes_;
  bool is_closed_;
  std::mutex mutex_close_;

  std::thread thread_buffer_handler_;
  EventManager *event_manager_;
  RateLimiter rate_limiter_;

  // Using a lock hierarchy to avoid deadlocks
  std::mutex mutex_live_write_level1_;
  std::mutex mutex_flush_level2_;
  std::mutex mutex_indices_level3_;
  std::mutex mutex_copy_write_level4_;
  std::mutex mutex_copy_read_level5_;
  std::condition_variable cv_flush_;
  std::condition_variable cv_flush_done_;
  std::condition_variable cv_read_;
};

} // namespace kdb

#endif // KINGDB_WRITE_BUFFER_H_
