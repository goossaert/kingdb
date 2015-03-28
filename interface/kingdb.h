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

// KingDB is an abstract class that serves as an interface for the two main classes
// that allow access to a KingDB database: Database and Snapshot.
class KingDB {
 friend class MultipartWriter;
 public:
  virtual ~KingDB() {}
  virtual Status Get(ReadOptions& read_options, ByteArray& key, ByteArray* value_out) = 0;

  virtual Status Get(ReadOptions& read_options, ByteArray& key, std::string* value_out) {
    ByteArray value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Get(ReadOptions& read_options, const std::string& key, ByteArray* value_out) {
    ByteArray byte_array_key = NewPointerByteArray(key.c_str(), key.size());
    Status s = Get(read_options, byte_array_key, value_out);
    return s;
  }

  virtual Status Get(ReadOptions& read_options, const std::string& key, std::string* value_out) {
    ByteArray byte_array_key = NewPointerByteArray(key.c_str(), key.size());
    ByteArray value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& chunk) = 0;

  virtual Status Put(WriteOptions& write_options, ByteArray& key, const std::string& chunk) {
    ByteArray byte_array_chunk = NewDeepCopyByteArray(chunk.c_str(), chunk.size());
    return Put(write_options, key, byte_array_chunk);
  }

  virtual Status Put(WriteOptions& write_options, const std::string& key, ByteArray& chunk) {
    ByteArray byte_array_key = NewDeepCopyByteArray(key.c_str(), key.size());
    return Put(write_options, byte_array_key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, const std::string& key, const std::string& chunk) {
    ByteArray byte_array_key = NewDeepCopyByteArray(key.c_str(), key.size());
    ByteArray byte_array_chunk = NewDeepCopyByteArray(chunk.c_str(), chunk.size());
    return Put(write_options, byte_array_key, byte_array_chunk);
  }

  virtual MultipartReader NewMultipartReader(ReadOptions& read_options, ByteArray& key) = 0;
  virtual Status Delete(WriteOptions& write_options, ByteArray& key) = 0;
  virtual Iterator NewIterator(ReadOptions& read_options) = 0;
  virtual Status Open() = 0;
  virtual void Close() = 0;
  virtual void Flush() = 0;
  virtual void Compact() = 0;

 private:
  virtual Status PutPart(WriteOptions& write_options,
                          ByteArray& key,
                          ByteArray& chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) = 0;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_H_
