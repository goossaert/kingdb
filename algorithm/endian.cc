#include "algorithm/endian.h"

namespace kdb {

endian_t getEndianness() {
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

const bool kLittleEndian = (getEndianness() == kBytesLittleEndian);
const bool kBigEndian = (getEndianness() == kBytesBigEndian);

} // namespace kdb
