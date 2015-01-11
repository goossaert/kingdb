// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_H_
#define KINGDB_INTERFACE_H_

#include "util/options.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "interface/iterator.h"

namespace kdb {

class Interface {
 public:
  virtual ~Interface() {};
  virtual Status Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) = 0;
  virtual Status Put(WriteOptions& write_options, ByteArray *key, ByteArray *chunk) = 0;
  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray *key,
                          ByteArray *chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) = 0;
  virtual Status Delete(WriteOptions& write_options, ByteArray *key) = 0;
  virtual Interface* NewSnapshot() = 0;
  virtual Iterator* NewIterator(ReadOptions& read_options) = 0;
  virtual Status Open() = 0;
  virtual void Close() = 0;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_H_
