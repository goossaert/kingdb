// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_OPTIONS_H_
#define KINGDB_OPTIONS_H_

namespace kdb {

enum EntryType {
  kPutEntry    = 0x0,
  kRemoveEntry = 0x1
};

enum HashType {
  kMurmurHash3_64 = 0x0,
  kxxHash_64      = 0x1
};

enum CompressionType {
  kNoCompression  = 0x0,
  kLZ4Compression = 0x1
};

struct CompressionOptions {
  CompressionOptions(CompressionType ct)
      : type(ct) {
  }
  CompressionType type;
};

struct DatabaseOptions {
  DatabaseOptions()
      : max_open_files(65535),
        hash(kxxHash_64),
        compression(kLZ4Compression) {
  }

  uint64_t max_open_files;
  HashType hash;
  CompressionOptions compression;
};

struct ReadOptions {
  ReadOptions() {
  }
};

struct WriteOptions {
  WriteOptions() {
  }
};


}

#endif // KINGDB_OPTIONS_H_
