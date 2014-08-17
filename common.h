// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMMON_H_
#define KINGDB_COMMON_H_

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "logger.h"
#include "status.h"

namespace kdb {

enum class OrderType { Put, Remove };

class ByteArray;

struct Order {
  OrderType type;
  ByteArray* key;
  ByteArray* chunk;
  uint64_t offset_chunk;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint32_t crc32;
};


struct Entry {
  uint32_t type;
  uint64_t size_key;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint64_t hash;
  uint32_t crc32;
};

struct EntryFooter {
  uint32_t crc32;
};

struct Metadata {
  uint32_t blocktype;
  uint64_t timestamp;
  uint64_t fileid_start;
  uint64_t fileid_end;
  uint64_t offset_compaction;
  uint64_t pointer_compaction;
};

struct LogFileFooter {
  uint64_t num_entries;
  uint64_t magic_number;
};

struct LogFileFooterIndex {
  uint64_t hashed_key;
  uint64_t offset_entry; // TODO: this only needs to be uint32_t really, but due to alignment/padding, I have set it to be uint64_t
};



}

#endif // KINGDB_COMMON_H_
