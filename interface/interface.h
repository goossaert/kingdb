// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_H_
#define KINGDB_INTERFACE_H_

#include "util/options.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"

namespace kdb {

class MultipartReader;
class Iterator;

/*
class Iterator {
 public:
  virtual ~Iterator() {}
  virtual void Begin() = 0;
  virtual bool IsValid() = 0;
  virtual bool Next() = 0;
  virtual ByteArray GetKey() = 0;
  virtual ByteArray GetValue() = 0;
  virtual MultipartReader GetMultipartValue() = 0;
};
*/


class Interface {
 public:
  virtual ~Interface() {}
  virtual Status Get(ReadOptions& read_options, ByteArray& key, ByteArray* value_out) = 0;

  virtual Status Get(ReadOptions& read_options, ByteArray& key, std::string* value_out) {
    ByteArray value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, ByteArray* value_out) {
    ByteArray byte_array_key = ByteArray::NewPointerByteArray(key.c_str(), key.size());
    Status s = Get(read_options, byte_array_key, value_out);
    return s;
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, std::string* value_out) {
    ByteArray byte_array_key = ByteArray::NewPointerByteArray(key.c_str(), key.size());
    ByteArray value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& chunk) = 0;

  virtual Status Put(WriteOptions& write_options, ByteArray& key, std::string& chunk) {
    ByteArray byte_array_chunk = ByteArray::NewDeepCopyByteArray(chunk.c_str(), chunk.size());
    return Put(write_options, key, byte_array_chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, ByteArray& chunk) {
    ByteArray byte_array_key = ByteArray::NewDeepCopyByteArray(key.c_str(), key.size());
    return Put(write_options, byte_array_key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, std::string& chunk) {
    ByteArray byte_array_key = ByteArray::NewDeepCopyByteArray(key.c_str(), key.size());
    ByteArray byte_array_chunk = ByteArray::NewDeepCopyByteArray(chunk.c_str(), chunk.size());
    return Put(write_options, byte_array_key, byte_array_chunk);
  }

  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray& key,
                          ByteArray& chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) = 0;
  virtual Status Delete(WriteOptions& write_options, ByteArray& key) = 0;
  virtual Iterator NewIterator(ReadOptions& read_options) = 0;
  virtual Status Open() = 0;
  virtual void Close() = 0;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_H_
