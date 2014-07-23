// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_H_
#define KINGDB_INTERFACE_H_

#include "status.h"
#include "common.h"
#include "byte_array.h"

namespace kdb {

class Interface {
 public:
  virtual Status Get(const std::string& key, ByteArray** value_out) = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status PutChunk(const char* key,
                          uint64_t size_key,
                          const char* chunk,
                          uint64_t size_chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value,
                          char * buffer_to_delete) = 0;
  virtual Status Remove(const char *key,
                        uint64_t size_key,
                        char * buffer_to_delete) = 0;

};

};

#endif // KINGDB_INTERFACE_H_
