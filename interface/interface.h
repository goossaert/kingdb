// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_H_
#define KINGDB_INTERFACE_H_

#include "util/options.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/kitten.h"

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
  virtual Kitten GetKey() = 0;
  virtual Kitten GetValue() = 0;
  virtual MultipartReader GetMultipartValue() = 0;
};
*/


class Interface {
 public:
  virtual ~Interface() {}
  virtual Status Get(ReadOptions& read_options, Kitten& key, Kitten* value_out) = 0;

  virtual Status Get(ReadOptions& read_options, Kitten& key, std::string* value_out) {
    Kitten value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, Kitten* value_out) {
    Kitten kitten_key = Kitten::NewPointerKitten(key.c_str(), key.size());
    Status s = Get(read_options, kitten_key, value_out);
    return s;
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, std::string* value_out) {
    Kitten kitten_key = Kitten::NewPointerKitten(key.c_str(), key.size());
    Kitten value;
    Status s = Get(read_options, key, &value);
    if (!s.IsOK()) return s;
    *value_out = value.ToString();
    return s;
  }

  virtual Status Put(WriteOptions& write_options, Kitten& key, Kitten& chunk) = 0;

  virtual Status Put(WriteOptions& write_options, Kitten& key, std::string& chunk) {
    Kitten kitten_chunk = Kitten::NewDeepCopyKitten(chunk.c_str(), chunk.size());
    return Put(write_options, key, kitten_chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, Kitten& chunk) {
    Kitten kitten_key = Kitten::NewDeepCopyKitten(key.c_str(), key.size());
    return Put(write_options, kitten_key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, std::string& chunk) {
    Kitten kitten_key = Kitten::NewDeepCopyKitten(key.c_str(), key.size());
    Kitten kitten_chunk = Kitten::NewDeepCopyKitten(chunk.c_str(), chunk.size());
    return Put(write_options, kitten_key, kitten_chunk);
  }

  virtual Status PutChunk(WriteOptions& write_options,
                          Kitten& key,
                          Kitten& chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) = 0;
  virtual Status Delete(WriteOptions& write_options, Kitten& key) = 0;
  virtual Interface* NewSnapshot() = 0;
  virtual Iterator NewIterator(ReadOptions& read_options) = 0;
  virtual Status Open() = 0;
  virtual void Close() = 0;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_H_
