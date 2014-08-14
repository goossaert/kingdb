// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "hash.h"

namespace kdb {

uint64_t MurmurHash3::HashFunction(char *data, uint32_t len) {
  // NOTE: You may need to change the seed, which by default is 0
  static char hash[16];
  static uint64_t output;
  // NOTE: Beware, the len in MurmurHash3_x64_128 is an 'int', not a 'uint32_t'
  MurmurHash3_x64_128(data, len, 0, hash);
  memcpy(&output, hash, 8); 
  return output;
}

uint64_t xxHash::HashFunction(char *data, uint32_t len) {
  // NOTE: You may need to change the seed, which by default is 0
  return XXH64(data, len, 0);
}

Hash* MakeHash(HashType ht) {
  if (ht == kMurmurHash3_64) {
    return new MurmurHash3();
  } else if (ht == kxxHash_64) {
    return new xxHash();
  } else {
    LOG_EMERG("Hash", "Unknown hashing function: [%d]", ht);
    exit(-1);
  }
}

}
