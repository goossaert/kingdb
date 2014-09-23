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
#include "util/crc32c.h"
#include "kingdb/byte_array_base.h"
#include "kingdb/byte_array.h"
#include "kingdb/options.h"

namespace kdb {

// Data format is version 1.0
static const uint32_t kVersionDataFormatMajor = 1;
static const uint32_t kVersionDataFormatMinor = 0;

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
    // Dumb serialization for debugging
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
    // Dumb serialization for debugging
    struct Entry *input_noncast = const_cast<struct Entry*>(input);
    memcpy(buffer, reinterpret_cast<char*>(input_noncast), sizeof(struct Entry));
    return sizeof(struct Entry);
    */

    // NOTE: it would be interesting to run an analysis and determine if it is
    // better to store the crc32 and hash using fixed encoding or varints. For
    // the hash, it will certainly be specific to each hash function.

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
  // NOTE: at first I wanted to have the CRC32 as part of an entry footer, the
  // since the compressed size has to be written in the header anyway, the
  // header has to be written twice, thus having a footer is not necessary.
  // I'm keeping this here to keep the idea in mind.
  uint32_t crc32;
};

enum FileType {
  kLogType   = 0x0,
  kLargeType = 0x1
};

struct LogFileHeader {
  uint32_t crc32;
  uint32_t version_data_format_major;
  uint32_t version_data_format_minor;
  uint32_t filetype;
  uint64_t timestamp;

  bool IsTypeLog() {
    return !(filetype & kLargeType);
  }

  bool IsTypeLarge() {
    return (filetype & kLargeType);
  }

  bool IsFileVersionSupported() {
    return (   version_data_format_major == kVersionDataFormatMajor
            && version_data_format_minor == kVersionDataFormatMinor);
  }

  bool IsFileVersionNewer() {
    if (   version_data_format_major > kVersionDataFormatMajor 
        || (   version_data_format_major == kVersionDataFormatMajor
            && version_data_format_minor > kVersionDataFormatMinor)
       ) {
      return true;
    }
    return false;
  }

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct LogFileHeader *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in     , &(output->crc32));
    GetFixed32(buffer_in +  4, &(output->version_data_format_major));
    GetFixed32(buffer_in +  8, &(output->version_data_format_minor));
    GetFixed32(buffer_in + 12, &(output->filetype));
    GetFixed64(buffer_in + 16, &(output->timestamp));
    uint32_t crc32_computed = crc32c::Value(buffer_in + 4, 20);
    if (!output->IsFileVersionSupported()) return Status::IOError("Data format version not supported");
    if (crc32_computed != output->crc32)   return Status::IOError("Invalid checksum");
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct LogFileHeader *input, char* buffer) {
    EncodeFixed32(buffer +  4, kVersionDataFormatMajor);
    EncodeFixed32(buffer +  8, kVersionDataFormatMinor);
    EncodeFixed32(buffer + 12, input->filetype);
    EncodeFixed64(buffer + 16, input->timestamp);
    uint32_t crc32 = crc32c::Value(buffer + 4, 20);
    EncodeFixed32(buffer, crc32);
    return GetFixedSize();
  }

  static uint32_t GetFixedSize() {
    return 24; // in bytes
  }
};

enum LogFileFooterFlags {
  kHasPaddingInValues = 0x1, // 1 if some values have size_value space but only use size_value_compressed and therefore need compaction, 0 otherwise
  kHasInvalidEntries  = 0x2  // 1 if some values have erroneous content that needs to be washed out in a compaction process -- will be set to 1 during a file recovery
};

struct LogFileFooter {
  uint32_t filetype;
  uint32_t flags;
  uint64_t offset_indexes;
  uint64_t num_entries;
  uint64_t magic_number;
  uint32_t crc32;

  LogFileFooter() { flags = 0; }

  void SetFlagHasPaddingInValues() {
    flags |= kHasPaddingInValues; 
  }

  void SetFlagHasInvalidEntries() {
    flags |= kHasInvalidEntries;
  }

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct LogFileFooter *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in,      &(output->filetype));
    GetFixed32(buffer_in +  4, &(output->flags));
    GetFixed64(buffer_in +  8, &(output->offset_indexes));
    GetFixed64(buffer_in + 16, &(output->num_entries));
    GetFixed64(buffer_in + 24, &(output->magic_number));
    GetFixed32(buffer_in + 32, &(output->crc32));
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct LogFileFooter *input, char* buffer) {
    EncodeFixed32(buffer,      input->filetype);
    EncodeFixed32(buffer +  4, input->flags);
    EncodeFixed64(buffer +  8, input->offset_indexes);
    EncodeFixed64(buffer + 16, input->num_entries);
    EncodeFixed64(buffer + 24, input->magic_number);
    // the checksum is computed in the method that writes the footer
    return GetFixedSize();
  }

  static uint32_t GetFixedSize() {
    return 36; // in bytes
  }
};

struct LogFileFooterIndex {
  uint64_t hashed_key;
  uint32_t offset_entry;

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct LogFileFooterIndex *output, uint32_t *num_bytes_read) {
    int length;
    char *buffer = const_cast<char*>(buffer_in);
    SimpleByteArray array(buffer, num_bytes_max);

    length = GetVarint64(&array, &(output->hashed_key));
    if (length == -1) return Status::IOError("Decoding error");
    array.AddOffset(length);

    length = GetVarint32(&array, &(output->offset_entry));
    if (length == -1) return Status::IOError("Decoding error");
    array.AddOffset(length);

    *num_bytes_read = num_bytes_max - array.size();
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct LogFileFooterIndex *input, char* buffer) {
    char *ptr;
    ptr = EncodeVarint64(buffer, input->hashed_key);
    ptr = EncodeVarint32(ptr, input->offset_entry);
    return (ptr - buffer);
  }
};



}

#endif // KINGDB_COMMON_H_
