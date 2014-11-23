// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

// The code below was copied from zlib and LevelDB. A few changes were
// applied to make it self-sufficient and part of KingDB.

// zlib.h -- interface of the 'zlib' general purpose compression library
// version 1.2.8, April 28th, 2013
// Copyright (C) 1995-2013 Jean-loup Gailly and Mark Adler

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef KINGDB_CRC32_H_
#define KINGDB_CRC32_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "util/logger.h"
#include "algorithm/endian.h"
#include "algorithm/coding.h"
#include "thread/threadstorage.h"

namespace kdb {
namespace crc32c {

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
extern uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t Value(const char* data, size_t n) {
  return Extend(0, data, n);
}

static const uint32_t kMaskDelta = 0xa282ead8ul;

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t Mask(uint32_t crc) {
  // Rotate right by 15 bits and add a constant.
  return ((crc >> 15) | (crc << 17)) + kMaskDelta;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t Unmask(uint32_t masked_crc) {
  uint32_t rot = masked_crc - kMaskDelta;
  return ((rot >> 17) | (rot << 15));
}


// For crc32_combine
typedef uint32_t ulong;
typedef int64_t I64;
ulong Combine(ulong crc1, ulong crc2, ulong len2);
#define GF2_DIM 32



}  // namespace crc32c


class CRC32 {
 public:
  CRC32() {}
  ~CRC32() {
  }

  // Added an empty copy assignment operator to avoid error messages of the type:
  // "object of type '...' cannot be assigned because its copy assignment
  //  operator is implicitly deleted"
  CRC32& operator=(const CRC32& r) {
    if(&r == this) return *this;
    return *this;
  }

  void stream(const char* data, size_t n) {
    //log::trace("CRC32", "size: %zu", n);
    uint64_t c = ts_.get();
    uint32_t c32 = c;
    uint32_t c_new = crc32c::Extend(c32, data, n);
    ts_.put(c_new);
  }

  uint32_t get() { return ts_.get(); }
  void put(uint32_t c32) { ts_.put(c32); }
  void ResetThreadLocalStorage() { ts_.reset(); }
   
 private:
  kdb::ThreadStorage ts_;
};

}  // namespace kdb 

#endif  // KINGDB_CRC32_H_
