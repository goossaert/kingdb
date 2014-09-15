// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

// The code below was copied from LevelDB. A few changes were applied to make it
// self-sufficient and part of KingDB.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef KINGDB_CRC32_H_
#define KINGDB_CRC32_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "util/logger.h"
#include "thread/threadstorage.h"

// TODO: The endian-ness detection code below is dependent on LevelDB's build
//       process, and I haven't yet made sure that it is supported on all
//       platforms.
#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(__APPLE__)
  #include <machine/endian.h>
  #if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
    #define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
  #endif
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define PLATFORM_IS_LITTLE_ENDIAN true
  #else
    #define PLATFORM_IS_LITTLE_ENDIAN false
  #endif
#elif defined(OS_FREEBSD)
  #include <sys/types.h>
  #include <sys/endian.h>
  #define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#elif defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD)
  #include <sys/types.h>
  #include <sys/endian.h>
#elif defined(OS_HPUX)
  #define PLATFORM_IS_LITTLE_ENDIAN false
#elif defined(OS_ANDROID)
  // Due to a bug in the NDK x86 <sys/endian.h> definition,
  // _BYTE_ORDER must be used instead of __BYTE_ORDER on Android.
  // See http://code.google.com/p/android/issues/detail?id=39824
  #include <endian.h>
  #define PLATFORM_IS_LITTLE_ENDIAN  (_BYTE_ORDER == _LITTLE_ENDIAN)
#else
  #include <endian.h>
#endif

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

namespace kdb {
namespace crc32c {

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

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

inline uint32_t DecodeFixed32(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}


}  // namespace crc32c

class CRC32 {
 public:
  CRC32() {}
  ~CRC32() {}

  // Added an empty copy assignment operator to avoid error messages of the type:
  // "object of type '...' cannot be assigned because its copy assignment
  //  operator is implicitly deleted"
  CRC32& operator=(const CRC32& r) {
    if(&r == this) return *this;
    return *this;
  }

  void stream(const char* data, size_t n) {
    LOG_TRACE("CRC32", "size: %zu", n);
    uint64_t c = ts_.get();
    uint32_t c32 = c;
    uint32_t c_new = crc32c::Extend(c32, data, n);
    ts_.put(c_new);
  }

  uint32_t get() { return ts_.get(); }
  void ResetThreadLocalStorage() { ts_.reset(); }
   
 private:
  kdb::ThreadStorage ts_;
};

}  // namespace kdb 

#endif  // KINGDB_CRC32_H_
