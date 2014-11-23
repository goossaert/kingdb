// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_FORMAT_H_
#define KINGDB_FORMAT_H_

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
#include "algorithm/coding.h"
#include "algorithm/crc32c.h"
#include "util/byte_array_base.h"
#include "util/byte_array.h"
#include "util/options.h"

namespace kdb {

// Data format is version 1.0
static const uint32_t kVersionDataFormatMajor = 1;
static const uint32_t kVersionDataFormatMinor = 0;

// 32-bit flags
// NOTE: kEntryFirst, kEntryMiddle and kEntryLast are not used yet,
//       they are reserved for possible future implementation.
enum EntryHeaderFlag {
  kTypeRemove    = 0x1,
  kHasPadding    = 0x2,
  kEntryFull     = 0x4,
  kEntryFirst    = 0x8,
  kEntryMiddle   = 0x10,
  kEntryLast     = 0x20
};


struct EntryHeader {
  EntryHeader() { flags = 0; }
  uint32_t crc32;
  uint32_t flags;
  uint64_t size_key;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint64_t hash;

  void print() {
    log::trace("EntryHeader::print()", "flags:%u crc32:%u size_key:%llu size_value:%llu size_value_compressed:%llu hash:%llu", flags, crc32, size_key, size_value, size_value_compressed, hash);
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
    log::trace("IsTypeRemove()", "flags %u", flags);
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


  static Status DecodeFrom(const DatabaseOptions& db_options, const char* buffer_in, uint64_t num_bytes_max, struct EntryHeader *output, uint32_t *num_bytes_read) {
    /*
    // Dumb serialization for debugging
    log::trace("EntryHeader::DecodeFrom", "start num_bytes_max:%llu - sizeof(EntryHeader):%d", num_bytes_max, sizeof(struct EntryHeader));
    char *buffer = const_cast<char*>(buffer_in);
    struct EntryHeader* entry_header = reinterpret_cast<struct EntryHeader*>(buffer);
    *output = *entry_header;
    *num_bytes_read = sizeof(struct EntryHeader);
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
    //log::trace("EntryHeader::DecodeFrom", "size:%u", *num_bytes_read);
    return Status::OK();
  }

  static uint32_t EncodeTo(const DatabaseOptions& db_options, const struct EntryHeader *input, char* buffer) {
    /*
    // Dumb serialization for debugging
    struct EntryHeader *input_noncast = const_cast<struct EntryHeader*>(input);
    memcpy(buffer, reinterpret_cast<char*>(input_noncast), sizeof(struct EntryHeader));
    return sizeof(struct EntryHeader);
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
    //log::trace("EntryHeader::EncodeTo", "size:%u", ptr - buffer + 8);
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
  kUnknownType        = 0x0,
  kUncompactedRegularType = 0x1,
  kCompactedRegularType   = 0x2,
  kCompactedLargeType = 0x4,
};

struct HSTableHeader {
  uint32_t crc32;
  uint32_t version_data_format_major;
  uint32_t version_data_format_minor;
  uint32_t filetype;
  uint64_t timestamp;

  HSTableHeader() {
    filetype = 0;
  }

  FileType GetFileType() {
    if (filetype & kCompactedLargeType) {
      return kCompactedLargeType;
    } else if (filetype & kCompactedRegularType) {
      return kCompactedRegularType;
    } else if (filetype & kUncompactedRegularType) {
      return kUncompactedRegularType;
    }
    return kUnknownType;
  }

  bool IsTypeLarge() {
    return (filetype & kCompactedLargeType);
  }

  bool IsTypeCompacted() {
    return (   filetype & kCompactedRegularType
            || filetype & kCompactedLargeType);
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

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct HSTableHeader *output) {
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

  static uint32_t EncodeTo(const struct HSTableHeader *input, char* buffer) {
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

enum HSTableFooterFlags {
  kHasPaddingInValues = 0x1, // 1 if some values have size_value space but only use size_value_compressed and therefore need compaction, 0 otherwise
  kHasInvalidEntries  = 0x2  // 1 if some values have erroneous content that needs to be washed out in a compaction process -- will be set to 1 during a file recovery
};

struct HSTableFooter {
  uint32_t filetype;
  uint32_t flags;
  uint64_t offset_indexes;
  uint64_t num_entries;
  uint64_t magic_number;
  uint32_t crc32;

  HSTableFooter() {
    flags = 0;
    filetype = 0;
  }

  bool IsTypeLarge() {
    return (filetype & kCompactedLargeType);
  }

  bool IsTypeCompacted() {
    return (   filetype & kCompactedRegularType
            || filetype & kCompactedLargeType);
  }
  
  void SetFlagHasPaddingInValues() {
    flags |= kHasPaddingInValues; 
  }

  void SetFlagHasInvalidEntries() {
    flags |= kHasInvalidEntries;
  }

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct HSTableFooter *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in,      &(output->filetype));
    GetFixed32(buffer_in +  4, &(output->flags));
    GetFixed64(buffer_in +  8, &(output->offset_indexes));
    GetFixed64(buffer_in + 16, &(output->num_entries));
    GetFixed64(buffer_in + 24, &(output->magic_number));
    GetFixed32(buffer_in + 32, &(output->crc32));
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct HSTableFooter *input, char* buffer) {
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


struct HSTableFooterIndex {
  uint64_t hashed_key;
  uint32_t offset_entry;

  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct HSTableFooterIndex *output, uint32_t *num_bytes_read) {
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

  static uint32_t EncodeTo(const struct HSTableFooterIndex *input, char* buffer) {
    char *ptr;
    ptr = EncodeVarint64(buffer, input->hashed_key);
    ptr = EncodeVarint32(ptr, input->offset_entry);
    return (ptr - buffer);
  }
};


struct DatabaseOptionEncoder {
  static Status DecodeFrom(const char* buffer_in, uint64_t num_bytes_max, struct DatabaseOptions *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");

    uint32_t crc32_computed = crc32c::Value(buffer_in + 4, 24);
    uint32_t crc32_stored; 
    GetFixed32(buffer_in, &crc32_stored);
    if (crc32_computed != crc32_stored) return Status::IOError("Invalid checksum");

    uint32_t version_data_format_major, version_data_format_minor;
    GetFixed32(buffer_in +  4, &version_data_format_major);
    GetFixed32(buffer_in +  8, &version_data_format_minor);
    if (   version_data_format_major != kVersionDataFormatMajor
        || version_data_format_minor != kVersionDataFormatMinor) {
      return Status::IOError("Data format version not supported");
    }

    uint32_t hash, compression_type;
    GetFixed64(buffer_in + 12, &(output->storage__hstable_size));
    GetFixed32(buffer_in + 20, &hash);
    GetFixed32(buffer_in + 24, &compression_type);
    if (hash == 0x0) {
      output->hash = kMurmurHash3_64;
    } else if (hash == 0x1) {
      output->hash = kxxHash_64;
    } else {
      return Status::IOError("Unknown hash type");
    }

    if (compression_type == 0x0) {
      output->compression.type = kNoCompression;
    } else if (compression_type == 0x1) {
      output->compression.type = kLZ4Compression;
    } else {
      return Status::IOError("Unknown compression type");
    }
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct DatabaseOptions *input, char* buffer) {
    EncodeFixed32(buffer +  4, kVersionDataFormatMajor);
    EncodeFixed32(buffer +  8, kVersionDataFormatMinor);
    EncodeFixed64(buffer + 12, input->storage__hstable_size);
    EncodeFixed32(buffer + 20, (uint32_t)input->hash);
    EncodeFixed32(buffer + 24, (uint32_t)input->compression.type);
    uint32_t crc32 = crc32c::Value(buffer + 4, 24);
    EncodeFixed32(buffer, crc32);
    return GetFixedSize();
  }

  static uint32_t GetFixedSize() {
    return 28; // in bytes
  }

};


} // namespace kdb

#endif // KINGDB_FORMAT_H_
