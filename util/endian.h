// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_ENDIAN_H_
#define KINGDB_ENDIAN_H_

namespace kdb {

enum endian_t : uint32_t {
    kBytesLittleEndian     = 0x00000001, // byte-swapped little-endian
    kBytesBigEndian        = 0x01000000, // byte-swapped big-endian
    kBytesLittleEndianWord = 0x00010000, // word-swapped little-endian
    kBytesBigEndianWord    = 0x00000100, // word-swapped big-endian
    kBytesUnknownEndian    = 0xffffffff
};

constexpr endian_t getEndianness() {
  if ((0xffffffff & 1) == kBytesLittleEndian) {
    return kBytesLittleEndian;
  } else if ((0xffffffff & 1) == kBytesBigEndian) {
    return kBytesBigEndian;
  } else if ((0xffffffff & 1) == kBytesLittleEndianWord) {
    return kBytesLittleEndianWord;
  } else if ((0xffffffff & 1) == kBytesBigEndianWord) {
    return kBytesBigEndianWord;
  }
  return kBytesUnknownEndian;
}

static const bool kLittleEndian = (getEndianness() == kBytesLittleEndian);
static const bool kBigEndian = (getEndianness() == kBytesBigEndian);

};

#endif // KINGDB_ENDIAN_H_
