// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_MULTIPART_H_
#define KINGDB_MULTIPART_H_

#include "util/debug.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>

#include <memory>
#include <string>
#include <string.h>

#include "util/logger.h"
#include "util/options.h"
#include "util/byte_array.h"
#include "algorithm/compressor.h"
#include "algorithm/crc32c.h"
#include "interface/kingdb.h"

namespace kdb {

class MultipartReader {
 friend class KingDB;
 friend class Snapshot;
 friend class Database;
 friend class RegularIterator;
 friend class SequentialIterator;
 public:
  ~MultipartReader() {}
 
  virtual void Begin() {
    log::trace("MultipartReader::Next()", "Begin()");
    if (read_options_.verify_checksums) {
      crc32_.ResetThreadLocalStorage();
      crc32_.put(value_.checksum_initial()); 
    }
    is_valid_stream_ = true;
    is_compression_disabled_ = false;
    offset_output_ = 0;
    compressor_.ResetThreadLocalStorage();
    status_ = Status::IOError("Stream is unfinished");
    Next();
  }

  virtual bool IsValid() {
    return is_valid_stream_;
  }

  virtual Status GetStatus() {
    log::trace("MultipartReader::GetStatus()", "");
    return status_; 
  }

  // Careful here: if the call to Next() is the first one, i.e. the one in
  // Begin(), then is_valid_stream_ must not be set to false yet, otherwise
  // the for-loops of type for(Begin(); IsValid(); Next()) would never run,
  // as IsValid() would prevent the first iteration to start.
  virtual bool Next() {
    if (is_compressed() && !is_compression_disabled_) {

      if (compressor_.IsUncompressionDone(value_.size_compressed())) {
        is_valid_stream_ = false;
        if (   !read_options_.verify_checksums
            || crc32_.get() == value_.checksum()) {
          log::debug("MultipartReader::Next()", "Good CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", value_.checksum(), crc32_.get());
          status_ = Status::OK();
        } else {
          log::debug("MultipartReader::Next()", "Bad CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", value_.checksum(), crc32_.get());
          status_ = Status::IOError("Invalid checksum.");
        }
        return true;
      }

      if (compressor_.HasFrameHeaderDisabledCompression(value_.data() + offset_output_)) {
        log::debug("MultipartReader::Next()", "Finds that compression is disabled\n");
        is_compression_disabled_ = true;
        if (read_options_.verify_checksums) {
          crc32_.stream(value_.data() + offset_output_, compressor_.size_frame_header());
        }
        offset_output_ += compressor_.size_frame_header();
      }

      if (!is_compression_disabled_) {
        char *frame;
        uint64_t size_frame;

        char *data_out;
        uint64_t size_out;

        log::trace("MultipartReader::Next()", "before uncompress");
        Status s = compressor_.Uncompress(value_.data(),
                                          value_.size_compressed(),
                                          &data_out,
                                          &size_out,
                                          &frame,
                                          &size_frame);
        offset_output_ += size_frame;
        chunk_ = NewShallowCopyByteArray(data_out, size_out);

        if (s.IsDone()) {
          is_valid_stream_ = false;
          status_ = Status::OK();
        } else if (s.IsOK()) {
          if (read_options_.verify_checksums) {
            crc32_.stream(frame, size_frame);
          }
        } else {
          is_valid_stream_ = false;
          status_ = s;
        }
      }
    }

    if (!value_.is_compressed() || is_compression_disabled_) {
      log::trace("MultipartReader::Next()", "No compression or compression disabled");
      uint64_t size_left;
      if (value_.is_compressed() && is_compression_disabled_) {
        size_left = value_.size_compressed();
      } else {
        size_left = value_.size();
      }

      if (offset_output_ == size_left) {
        log::trace("MultipartReader::Next()", "Has gotten all the data");
        is_valid_stream_ = false;
        status_ = Status::OK();
        return true;
      }

      char* data_left = value_.data() + offset_output_;

      size_t step = 1024*1024;
      size_t size_current = offset_output_ + step < size_left ? step : size_left - offset_output_;
      if (read_options_.verify_checksums) {
        crc32_.stream(data_left, size_current);
      }

      chunk_ = value_;
      chunk_.increment_offset(offset_output_);
      chunk_.set_size(size_current);
      chunk_.set_size_compressed(0);
      offset_output_ += size_current;
      status_ = Status::OK();
      log::trace("MultipartReader::Next()", "Done with handling uncompressed data - Status:%s", status_.ToString().c_str());
    }
    return true;
  }

  virtual Status GetPart(ByteArray* part) {
    log::trace("MultipartReader::Next()", "GetPart() - Status:%s", status_.ToString().c_str());
    *part = chunk_;
    return status_;
  }

  CompressorLZ4 compressor_;
  CRC32 crc32_;
  uint64_t offset_output_;
  bool is_compression_disabled_;
 
  Status status_;
  ByteArray chunk_;
  bool is_valid_stream_;

  bool is_compressed() {
    return value_.is_compressed();
  }

  MultipartReader(const MultipartReader& r) {
    if(&r != this) {
      this->read_options_ = r.read_options_;
      this->value_ = r.value_;
    }
  }

  uint64_t size() { return value_.size(); }

 private:

  MultipartReader(Status s)
    : status_(s) {
  }
  MultipartReader(ReadOptions& read_options, ByteArray& value)
    : read_options_(read_options),
      value_(value),
      status_(Status::OK()) {
  }

  ReadOptions read_options_;
  ByteArray value_;
};


class MultipartWriter {
 friend class Database;
 public:
  ~MultipartWriter() {}

  Status PutPart(ByteArray& part) {
    Status s = db_->PutPart(write_options_, key_, part, offset_, size_value_total_);
    if (s.IsOK()) offset_ += part.size();
    return s;
  }
 private:
  MultipartWriter(KingDB* db, WriteOptions& write_options, ByteArray& key, uint64_t size_value_total)
    : db_(db),
      write_options_(write_options),
      key_(key),
      size_value_total_(size_value_total),
      offset_(0) {
  }

  KingDB* db_;
  WriteOptions write_options_;
  ByteArray key_;
  uint64_t size_value_total_;
  uint64_t offset_;
};



} // namespace kdb

#endif // KINGDB_MULTIPART_H_
