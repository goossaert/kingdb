// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_ORDER_H_
#define KINGDB_ORDER_H_

#include "util/debug.h"

#include <set>

#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "util/logger.h"
#include "util/status.h"
#include "algorithm/coding.h"
#include "algorithm/crc32c.h"
#include "util/byte_array_base.h"
#include "util/byte_array.h"
#include "util/options.h"

namespace kdb {

enum class OrderType { Put, Remove };

struct Order {
  std::thread::id tid;
  OrderType type;
  ByteArray* key;
  ByteArray* chunk;
  uint64_t offset_chunk;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint32_t crc32;
  bool is_large;

  bool IsFirstChunk() {
    return (offset_chunk == 0);
  }

  bool IsMiddleOrLastChunk() {
    return !IsFirstChunk();
  }

  bool IsLastChunk() {
    return (   (size_value_compressed == 0 && chunk->size() + offset_chunk == size_value)
            || (size_value_compressed != 0 && chunk->size() + offset_chunk == size_value_compressed));
  }

  bool IsSelfContained() {
    return IsFirstChunk() && IsLastChunk();
  }

  bool IsLarge() {
    return is_large; 
  }
};

} // namespace kdb

#endif // KINGDB_ORDER_H_
