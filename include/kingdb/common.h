// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMMON_H_
#define KINGDB_COMMON_H_

#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "util/logger.h"
#include "util/status.h"
#include "util/coding.h"
#include "kingdb/byte_array_base.h"
#include "kingdb/byte_array.h"
#include "kingdb/options.h"

namespace kdb {

enum class OrderType { Put, Remove };

//class ByteArray;

struct Order {
  std::thread::id tid;
  OrderType type;
  ByteArray* key;
  ByteArray* chunk;
  uint64_t offset_chunk;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint32_t crc32;

  bool IsFirstChunk() {
    return (offset_chunk == 0);
  }

  bool IsLastChunk() {
    return (   (size_value_compressed == 0 && chunk->size() + offset_chunk == size_value)
            || (size_value_compressed != 0 && chunk->size() + offset_chunk == size_value_compressed));
  }

  bool IsSelfContained() {
    return IsFirstChunk() && IsLastChunk();
  }
};

// 32-bit flags
// NOTE: kEntryFirst, kEntryMiddle and kEntryLast are not used yet,
//       they are reserved for possible future implementation.
enum EntryFlag {
  kTypeRemove    = 0x1,
  kHasPadding    = 0x2,
  kEntryFull     = 0x4,
  kEntryFirst    = 0x8,
  kEntryMiddle   = 0x10,
  kEntryLast     = 0x20
};


struct Entry {
  Entry() { flags = 0; }
  uint32_t crc32;
  uint32_t flags;
  uint64_t size_key;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint64_t hash;

  void print() {
    LOG_TRACE("Entry::print()", "flags:%u crc32:%u size_key:%llu size_value:%llu size_value_compressed:%llu hash:%llu", flags, crc32, size_key, size_value, size_value_compressed, hash);
  }

  void SetHasPadding(bool b) {
    if (b) {
      flags |= kHasPadding;
    } else {
      flags &= ~kHasPadding; 
    }
  }

  bool HasPadding() {
    return (flags & kHasPadding);
  }

  void SetTypeRemove() {
    flags |= kTypeRemove; 
  }

  void SetTypePut() {
    // do nothing
  }

  bool IsTypeRemove() {
    LOG_TRACE("IsTypeRemove()", "flags %u", flags);
    return (flags & kTypeRemove);
  }
  
  bool IsTypePut() {
    return !IsTypeRemove();
  }

  void SetEntryFull() {
    flags |= kEntryFull; 
  }

  bool IsEntryFull() {
    return (flags & kEntryFull); 
  }

  bool IsCompressed() {
    return (size_value_compressed > 0); 
  }

  uint64_t size_value_used() {
    if (IsCompressed()) {
      return size_value_compressed;
    } else {
      return size_value;
    }
  }

  uint64_t size_value_offset() {
    if (!IsCompressed() || HasPadding()) {
      return size_value;
    } else {
      return size_value_compressed;
    }
  }


  static Status DecodeFrom(const DatabaseOptions& db_options, const char* buffer_in, uint64_t num_bytes_max, struct Entry *output, uint32_t *num_bytes_read) {
    /*
    LOG_TRACE("Entry::DecodeFrom", "start num_bytes_max:%llu - sizeof(Entry):%d", num_bytes_max, sizeof(struct Entry));
    char *buffer = const_cast<char*>(buffer_in);
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer);
    *output = *entry;
    *num_bytes_read = sizeof(struct Entry);
    return Status::OK();
    */

    int length;
    char *buffer = const_cast<char*>(buffer_in);
    SimpleByteArray array(buffer, num_bytes_max);

    GetFixed32(array.data(), &(output->crc32));
    array.AddOffset(4);

    length = GetVarint32(&array, &(output->flags));
    if (length == -1) return Status::IOError("Decoding error");
    array.AddOffset(length);

    length = GetVarint64(&array, &(output->size_key));
    if (length == -1) return Status::IOError("Decoding error");
    array.AddOffset(length);

    int length_value = GetVarint64(&array, &(output->size_value));
    if (length_value == -1) return Status::IOError("Decoding error");
    array.AddOffset(length_value);

    if (db_options.compression.type != kNoCompression) {
      int length = GetVarint64(&array, &(output->size_value_compressed));
      if (length == -1) return Status::IOError("Decoding error");
      array.AddOffset(length_value); // size_value_compressed is using length_value
    }

    if (array.size() < 8) return Status::IOError("Decoding error");
    GetFixed64(array.data(), &(output->hash));

    *num_bytes_read = num_bytes_max - array.size() + 8;
    //LOG_TRACE("Entry::DecodeFrom", "size:%u", *num_bytes_read);
    return Status::OK();
  }

  static uint32_t EncodeTo(const DatabaseOptions& db_options, const struct Entry *input, char* buffer) {
    /*
    struct Entry *input_noncast = const_cast<struct Entry*>(input);
    memcpy(buffer, reinterpret_cast<char*>(input_noncast), sizeof(struct Entry));
    //struct Entry *entry = reinterpret_cast<struct Entry*>(buffer);
    //*entry = *input;
    return sizeof(struct Entry);
    */

    EncodeFixed32(buffer, input->crc32);
    char *ptr;
    ptr = EncodeVarint32(buffer + 4, input->flags);
    ptr = EncodeVarint64(ptr, input->size_key);
    char *ptr_value = EncodeVarint64(ptr, input->size_value);
    int length_value = ptr_value - ptr;
    ptr = ptr_value;
    if (db_options.compression.type != kNoCompression) {
      // size_value_compressed is stored only if the database is using compression
      if (input->size_value_compressed != 0) {
        EncodeVarint64(ptr, input->size_value_compressed);
      }
      ptr += length_value;
    }
    EncodeFixed64(ptr, input->hash);
    //LOG_TRACE("Entry::EncodeTo", "size:%u", ptr - buffer + 8);
    return (ptr - buffer + 8);
  }

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

enum FileType {
  kLogType   = 0x0,
  kLargeType = 0x1
};

struct LogFileHeader {
  uint32_t filetype;
  uint64_t timestamp;
};

struct LogFileFooter {
  uint32_t filetype;
  uint64_t num_entries;
  uint16_t has_padding_in_values; // 1 if some values have size_value space but only use size_value_compressed and therefore need compaction, 0 otherwise
  uint16_t has_invalid_entries;   // 1 if some values have erroneous content that needs to be washed out in a compaction process -- will be set to 1 during a file recovery
  uint64_t magic_number;
};

struct LogFileFooterIndex {
  uint64_t hashed_key;
  uint64_t offset_entry; // TODO: this only needs to be uint32_t really, but due to alignment/padding, I have set it to be uint64_t
};



}

#endif // KINGDB_COMMON_H_
