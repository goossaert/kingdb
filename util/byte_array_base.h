// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_BYTE_ARRAY_BASE_H_
#define KINGDB_BYTE_ARRAY_BASE_H_

#include "util/debug.h"
#include "util/status.h"

namespace kdb {

class ByteArray {
 public:
  ByteArray() {}
  virtual ~ByteArray() {}
  virtual std::string ToString() = 0;
  virtual char* data() = 0;
  virtual char* data_const() const = 0;
  virtual uint64_t size() = 0;
  virtual uint64_t size_const() const = 0;
  virtual uint64_t size_compressed() = 0;
  virtual uint64_t size_compressed_const() const = 0;
  virtual uint32_t checksum() const = 0;
  virtual uint64_t offset() const = 0;
  virtual void set_offset(int off) = 0;
  virtual bool is_compressed() = 0;
  virtual void SetOffset(uint64_t offset, uint64_t size) = 0;
  virtual bool StartsWith(const char *substr, int n) = 0;
  virtual void SetSizes(uint64_t size, uint64_t size_compressed) = 0;

  // ByteArray management
  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) = 0;
  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) = 0;

  // Streaming API
  virtual void Begin() = 0;
  virtual bool IsValid() = 0;
  virtual bool Next() = 0;
  virtual ByteArray* GetChunk() = 0;
  virtual Status GetStatus() = 0;

  bool operator ==(const ByteArray &right) const {
    //fprintf(stderr, "ByteArray operator==() -- left: %p %" PRIu64 " [%s] right: %p %" PRIu64 " [%s]\n", data_, size_, std::string(data_, size_).c_str(), right.data_const(), right.size_const(), std::string(right.data_const(), right.size_const()).c_str());
    return (   size_const() == right.size_const()
            && memcmp(data_const(), right.data_const(), size_const()) == 0);
  }

};

};

#endif // KINGDB_BYTE_ARRAY_BASE_H_

