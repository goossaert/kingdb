// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_FORMAT_H_
#define KINGDB_FORMAT_H_

#include "util/debug.h"
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include "util/version.h"
#include "util/logger.h"
#include "util/status.h"
#include "algorithm/coding.h"
#include "algorithm/crc32c.h"
#include "util/options.h"

namespace kdb {

// Data format is version 1.0
static const uint32_t kVersionDataFormatMajor   = 1;
static const uint32_t kVersionDataFormatMinor   = 0;

// 32-bit flags
// NOTE: kEntryFirst, kEntryMiddle and kEntryLast are not used yet,
//       they are reserved for possible future implementation.
enum EntryHeaderFlag {
  kTypeDelete    = 0x1,
  kIsUncompacted = 0x2,
  kHasPadding    = 0x4,
  kEntryFull     = 0x8,
  kEntryFirst    = 0x10,
  kEntryMiddle   = 0x20,
  kEntryLast     = 0x40
};


struct EntryHeader {
  EntryHeader() { flags = 0; }
  uint8_t  checksum_header;
  uint32_t checksum_content;
  uint32_t flags;
  uint64_t size_key;
  uint64_t size_value;
  uint64_t size_value_compressed;
  uint64_t size_padding;
  uint64_t hash;

  // Helpers, not store on secondary storage
  int32_t size_header_serialized;

  void print() {
    log::trace("EntryHeader::print()", "flags:%u checksum_content:0x%08" PRIx64 " size_key:%" PRIu64 " size_value:%" PRIu64 " size_value_compressed:%" PRIu64 " size_padding:%" PRIu64  " hash:0x%08" PRIx64, flags, checksum_content, size_key, size_value, size_value_compressed, size_padding, hash);
  }

  static uint64_t CalculatePaddingSize(uint64_t size_value) {
    // NOTE: Here I picked an arbitrary frame size of 64KB, and do an estimate
    // of the padding necessary for the frame headers based on the current size
    // of the value.
    // This logic is related to the compression algorithms, and therefore should
    // be moved to the compression classes.
    uint64_t size_frame_header = 8;
    return (size_value / (64*1024) + 1) * size_frame_header;
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

  void SetIsUncompacted(bool b) {
    if (b) {
      flags |= kIsUncompacted;
    } else {
      flags &= ~kIsUncompacted;
    }
  }

  bool IsUncompacted() {
    return (flags & kIsUncompacted);
  }

  void SetTypeDelete() {
    flags |= kTypeDelete; 
  }

  void SetTypePut() {
    // do nothing
  }

  bool AreSizesValid(uint32_t offset_file, uint64_t filesize) {
    return (   size_key > 0
            && offset_file + size_header_serialized + size_key + size_value_offset() <= filesize);
  }

  bool IsTypeDelete() {
    log::trace("IsTypeDelete()", "flags %u", flags);
    return (flags & kTypeDelete);
  }
  
  bool IsTypePut() {
    return !IsTypeDelete();
  }

  void SetEntryFull() {
    flags |= kEntryFull; 
  }

  bool IsEntryFull() {
    log::trace("IsEntryFull()", "flags %u", flags);
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
    if (!IsCompressed() || IsUncompacted()) {
      return size_value + size_padding;
    } else {
      return size_value_compressed;
    }
  }

  static Status DecodeFrom(const DatabaseOptions& db_options,
                           const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct EntryHeader *output,
                           uint32_t *num_bytes_read) {
    /*
    // Dumb serialization for debugging
    log::trace("EntryHeader::DecodeFrom", "start num_bytes_max:%" PRIu64 " - sizeof(EntryHeader):%d", num_bytes_max, sizeof(struct EntryHeader));
    char *buffer = const_cast<char*>(buffer_in);
    struct EntryHeader* entry_header = reinterpret_cast<struct EntryHeader*>(buffer);
    *output = *entry_header;
    *num_bytes_read = sizeof(struct EntryHeader);
    return Status::OK();
    */

    int length;
    char *buffer = const_cast<char*>(buffer_in);
    char *ptr = buffer;
    int size = num_bytes_max;

    output->checksum_header = ptr[0];
    ptr += 1;
    size -= 1;
    
    GetFixed32(ptr, &(output->checksum_content));
    ptr += 4;
    size -= 4;

    length = GetVarint32(ptr, size, &(output->flags));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    length = GetVarint64(ptr, size, &(output->size_key));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    length = GetVarint64(ptr, size, &(output->size_value));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    if (db_options.compression.type != kNoCompression) {
      GetFixed64(ptr, &(output->size_value_compressed));
      ptr += 8;
      size -= 8;
      length = GetVarint64(ptr, size, &(output->size_padding));
      if (length == -1) return Status::IOError("Decoding error");
      ptr += length;
      size -= length;
    } else {
      output->size_value_compressed = 0;
      output->size_padding = 0;
    }

    if (size < 8) return Status::IOError("Decoding error");
    GetFixed64(ptr, &(output->hash));
    ptr += 8;
    size -= 8;

    *num_bytes_read = num_bytes_max - size;
    output->size_header_serialized = *num_bytes_read;

    uint8_t checksum_header = crc32c::crc8(0, buffer + 1, output->size_header_serialized - 1);
    if (checksum_header != output->checksum_header) {
      return Status::IOError("Header checksum mismatch");
    }

    //log::trace("EntryHeader::DecodeFrom", "size:%u", *num_bytes_read);
    return Status::OK();
  }

  static uint32_t EncodeTo(const DatabaseOptions& db_options,
                           const struct EntryHeader *input,
                           char* buffer) {
    /*
    // Dumb serialization for debugging
    struct EntryHeader *input_noncast = const_cast<struct EntryHeader*>(input);
    memcpy(buffer, reinterpret_cast<char*>(input_noncast), sizeof(struct EntryHeader));
    return sizeof(struct EntryHeader);
    */

    // NOTE: it would be interesting to run an analysis and determine if it is
    // better to store the crc32 and hash using fixed encoding or varints. For
    // the hash, it will certainly be specific to each hash function.
    char *ptr = buffer + 1; // save 1 byte for the header checksum
    EncodeFixed32(ptr, input->checksum_content);
    ptr = EncodeVarint32(ptr + 4, input->flags);
    ptr = EncodeVarint64(ptr, input->size_key);
    ptr = EncodeVarint64(ptr, input->size_value);
    if (db_options.compression.type != kNoCompression) {
      EncodeFixed64(ptr, input->size_value_compressed);
      ptr += 8;
      ptr = EncodeVarint64(ptr, input->size_padding);
    }
    EncodeFixed64(ptr, input->hash);
    ptr += 8;

    buffer[0] = crc32c::crc8(0, buffer + 1, (ptr-buffer) - 1);

    //log::trace("EntryHeader::EncodeTo", "size:%u", ptr - buffer);
    //PrintHex(buffer, ptr - buffer);
    return (ptr - buffer);
  }

};

struct EntryFooter {
  // NOTE: at first I wanted to have the CRC32 as part of an entry footer, the
  // since the compressed size has to be written in the header anyway, the
  // header has to be written twice, thus having a footer is not necessary.
  // I'm keeping this here to keep the idea in mind.
  uint32_t crc32;
};


struct DatabaseOptionEncoder {
  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct DatabaseOptions *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");

    uint32_t crc32_computed = crc32c::Value(buffer_in + 4, GetFixedSize() - 4);
    uint32_t crc32_stored; 
    GetFixed32(buffer_in, &crc32_stored);
    if (crc32_computed != crc32_stored) return Status::IOError("Invalid checksum");

    uint32_t version_data_format_major, version_data_format_minor;
    GetFixed32(buffer_in + 20, &version_data_format_major);
    GetFixed32(buffer_in + 24, &version_data_format_minor);
    if (   version_data_format_major != kVersionDataFormatMajor
        || version_data_format_minor != kVersionDataFormatMinor) {
      return Status::IOError("Data format version not supported");
    }

    uint32_t hash, compression_type, checksum_type;
    GetFixed64(buffer_in + 28, &(output->storage__hstable_size));
    GetFixed32(buffer_in + 36, &hash);
    GetFixed32(buffer_in + 40, &compression_type);
    GetFixed32(buffer_in + 44, &checksum_type);
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

    if (compression_type == 0x1) {
      output->checksum = kCRC32C;
    } else {
      return Status::IOError("Unknown checksum type");
    }

    return Status::OK();
  }

  static uint32_t EncodeTo(const struct DatabaseOptions *input, char* buffer) {
    EncodeFixed32(buffer +  4, kVersionMajor);
    EncodeFixed32(buffer +  8, kVersionMinor);
    EncodeFixed32(buffer + 12, kVersionRevision);
    EncodeFixed32(buffer + 16, kVersionBuild);
    EncodeFixed32(buffer + 20, kVersionDataFormatMajor);
    EncodeFixed32(buffer + 24, kVersionDataFormatMinor);
    EncodeFixed64(buffer + 28, input->storage__hstable_size);
    EncodeFixed32(buffer + 36, (uint32_t)input->hash);
    EncodeFixed32(buffer + 40, (uint32_t)input->compression.type);
    EncodeFixed32(buffer + 44, (uint32_t)input->checksum);
    uint32_t crc32 = crc32c::Value(buffer + 4, GetFixedSize() - 4);
    EncodeFixed32(buffer, crc32);
    return GetFixedSize();
  }

  static uint32_t GetFixedSize() {
    return 48; // in bytes
  }

};


enum FileType {
  kUnknownType            = 0x0,
  kUncompactedRegularType = 0x1,
  kCompactedRegularType   = 0x2,
  kCompactedLargeType     = 0x4,
};

struct HSTableHeader {
  uint32_t crc32;
  uint32_t version_major;
  uint32_t version_minor;
  uint32_t version_revision;
  uint32_t version_build;
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

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct HSTableHeader *output,
                           struct DatabaseOptions *db_options_out=nullptr) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in     , &(output->crc32));
    GetFixed32(buffer_in +  4, &(output->version_data_format_major));
    GetFixed32(buffer_in +  8, &(output->version_data_format_minor));
    GetFixed32(buffer_in + 12, &(output->filetype));
    GetFixed64(buffer_in + 16, &(output->timestamp));
    uint32_t crc32_computed = crc32c::Value(buffer_in + 4, 20);
    if (!output->IsFileVersionSupported()) return Status::IOError("Data format version not supported");
    if (crc32_computed != output->crc32)   return Status::IOError("Invalid checksum");

    if (db_options_out == nullptr) return Status::OK();
    Status s = DatabaseOptionEncoder::DecodeFrom(buffer_in + GetFixedSize(), num_bytes_max - GetFixedSize(), db_options_out);
    return s;
  }

  static uint32_t EncodeTo(const struct HSTableHeader *input, const struct DatabaseOptions* db_options, char* buffer) {
    EncodeFixed32(buffer +  4, kVersionDataFormatMajor);
    EncodeFixed32(buffer +  8, kVersionDataFormatMinor);
    EncodeFixed32(buffer + 12, input->filetype);
    EncodeFixed64(buffer + 16, input->timestamp);
    uint32_t crc32 = crc32c::Value(buffer + 4, 20);
    EncodeFixed32(buffer, crc32);
    int size_db_options = DatabaseOptionEncoder::EncodeTo(db_options, buffer + GetFixedSize());
    return GetFixedSize() + size_db_options;
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

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct HSTableFooter *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in,      &(output->filetype));
    GetFixed32(buffer_in +  4, &(output->flags));
    GetFixed64(buffer_in +  8, &(output->offset_indexes));
    GetFixed64(buffer_in + 16, &(output->num_entries));
    GetFixed64(buffer_in + 24, &(output->magic_number));
    GetFixed32(buffer_in + 32, &(output->crc32));
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct HSTableFooter *input,
                           char* buffer) {
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


struct OffsetArrayRow {
  uint64_t hashed_key;
  uint32_t offset_entry;

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct OffsetArrayRow *output,
                           uint32_t *num_bytes_read) {
    int length;
    char *ptr = const_cast<char*>(buffer_in);
    int size = num_bytes_max;

    length = GetVarint64(ptr, size, &(output->hashed_key));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    length = GetVarint32(ptr, size, &(output->offset_entry));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    *num_bytes_read = num_bytes_max - size;
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct OffsetArrayRow *input, char* buffer) {
    char *ptr;
    ptr = EncodeVarint64(buffer, input->hashed_key);
    ptr = EncodeVarint32(ptr, input->offset_entry);
    return (ptr - buffer);
  }
};


} // namespace kdb

#endif // KINGDB_FORMAT_H_
