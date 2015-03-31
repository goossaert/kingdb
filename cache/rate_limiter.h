// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_RATE_LIMITER_H_
#define KINGDB_RATE_LIMITER_H_

#include "util/debug.h"
#include <cinttypes>

namespace kdb {

class RateLimiter {
 public:
  RateLimiter(uint64_t rate_limit)
    : rate_limit_(rate_limit),
      rate_incoming_(250 * 1024 * 1024),
      rate_incoming_adjusted_(0),
      rate_writing_default_(1 * 1024 * 1024),
      epoch_last_(0),
      epoch_current_(0),
      duration_slept_(0),
      bytes_per_microsecond_(5)
  {
  }

  ~RateLimiter() {
  }

  void Tick(uint64_t bytes_incoming) {
    epoch_current_ = std::time(0);
    if (epoch_current_ != epoch_last_) {
      rate_incoming_adjusted_ = rate_incoming_ + bytes_per_microsecond_ * duration_slept_;
      log::trace("RateLimiter::Tick()",
                 "rate_incoming_: %" PRIu64 " rate_incoming_adjusted_:%" PRIu64,
                 rate_incoming_, rate_incoming_adjusted_);
      duration_slept_ = 0;
      rate_incoming_ = 0;
      epoch_last_ = epoch_current_;

      uint64_t rate_writing = GetWritingRate();
      double ratio = (double)rate_incoming_adjusted_ / (double)rate_writing;
      if (ratio > 1.0) {
        // The rate of incoming data is greater than the rate at which data
        // can be written, therefore the number of bytes for each microsecond
        // slept must be decreased: this means that less bytes will trigger the
        // same amount of microseconds slept, which will increase the amount
        // of time spent sleeping, and bring the rate of incoming data closer
        // to the rate at which data is written.
        if (ratio > 1.50) {
          bytes_per_microsecond_ *= 0.75;
        } else if (ratio > 1.10) {
          bytes_per_microsecond_ *= 0.95;
        } else if (ratio > 1.05) {
          bytes_per_microsecond_ *= 0.99;
        } else {
          bytes_per_microsecond_ *= 0.995;
        }
        if (bytes_per_microsecond_ <= 5) bytes_per_microsecond_ += 1;
        log::trace("RateLimiter::Tick()", "decreasing");
      } else {
        // The rate of incoming data is lower than the rate at which data
        // can be written, therefore bytes_per_microsecond_ needs to be
        // increased.
        if (ratio < 0.5) {
          bytes_per_microsecond_ *= 1.25;
        } else if (ratio < 0.90) {
          bytes_per_microsecond_ *= 1.05;
        } else if (ratio < 0.95) {
          bytes_per_microsecond_ *= 1.01;
        } else {
          bytes_per_microsecond_ *= 1.005;
        }
        log::trace("RateLimiter::Tick()", "increasing");
        if (bytes_per_microsecond_ <= 5) bytes_per_microsecond_ += 1;
      }

      log::trace("RateLimiter::Tick()",
                 "limit rate: bytes_per_microsecond_: %" PRIu64 " rate_writing:%" PRIu64,
                 bytes_per_microsecond_, rate_writing);
    }

    mutex_throttling_.lock();
    rate_incoming_ += bytes_incoming;
    uint64_t sleep_microseconds = 0;
    if (bytes_per_microsecond_ > 0) {
      sleep_microseconds = bytes_incoming / bytes_per_microsecond_;
    }
    mutex_throttling_.unlock();
    if (sleep_microseconds > 50000) sleep_microseconds = 50000;

    if (sleep_microseconds) {
      log::trace("RateLimiter::Tick()",
                 "bytes_per_microsecond_: %" PRIu64 ", sleep_microseconds: %" PRIu64,
                 bytes_per_microsecond_, sleep_microseconds);
      std::chrono::microseconds duration(sleep_microseconds);
      std::this_thread::sleep_for(duration);
      duration_slept_ += sleep_microseconds;
    }
  }


  void WriteStart() {
    auto epoch = std::chrono::system_clock::now().time_since_epoch();
    epoch_write_start_ = std::chrono::duration_cast<std::chrono::milliseconds>(epoch).count();
  }


  void WriteEnd(uint64_t num_bytes_written) {
    auto epoch = std::chrono::system_clock::now().time_since_epoch();
    epoch_write_end_ = std::chrono::duration_cast<std::chrono::milliseconds>(epoch).count();
    uint64_t rate_writing = num_bytes_written;
    mutex_throttling_.lock();
    if (epoch_write_start_ == epoch_write_end_) {
      rate_writing = num_bytes_written;
    } else {
      double duration = ((double)epoch_write_end_ - (double)epoch_write_start_) / 1000;
      rate_writing = (uint64_t)((double)num_bytes_written / (double)duration);
    }
    StoreWritingRate(rate_writing);
    mutex_throttling_.unlock();
  }


  void StoreWritingRate(uint64_t rate) {
    if (rates_.size() >= 10) {
      rates_.erase(rates_.begin());
    }
    rates_.push_back(rate);
  }

  uint64_t GetWritingRate() {
    // If no rate has been stored yet, a default value is used.
    if (rates_.size() == 0) return rate_writing_default_;
    // The writting rate is an average: this allows to cope with irregularities
    // in the throughput and prevent the rate limiter to fall into a flapping
    // effect, in which it would limit the throughput either way too much,
    // or not enough. This average allow the bandwitdth to converge smoothly.
    uint64_t sum = 0; 
    for (int i = 0; i < rates_.size(); i++) {
      sum += rates_[i]; 
    }
    uint64_t rate_limit_current = sum / rates_.size();
    if (rate_limit_ > 0 && rate_limit_ < rate_limit_current) {
      return rate_limit_;
    } else if (rate_limit_current > 0){
      return rate_limit_current;
    } else {
      return rate_writing_default_; 
    }
  }

  uint64_t epoch_write_start_;
  uint64_t epoch_write_end_;
  uint64_t rate_limit_;
  uint64_t rate_incoming_;
  uint64_t rate_incoming_adjusted_;
  uint64_t rate_writing_;
  uint64_t rate_writing_default_;
  uint64_t epoch_last_;
  uint64_t epoch_current_;
  uint64_t duration_slept_;
  uint64_t bytes_per_microsecond_;
  std::mutex mutex_throttling_;
  std::vector<uint64_t> rates_;
};

} // namespace kdb

#endif // KINGDB_RATE_LIMITER_H_
