// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_BYTE_ARRAY_H_
#define KINGDB_BYTE_ARRAY_H_

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
#include "algorithm/compressor.h"
#include "algorithm/crc32c.h"

#include "util/byte_array_base.h"

namespace kdb {

class Stream {
 public:
  Stream(ByteArray* byte_array)
    : byte_array_(byte_array),
      chunk_current_(nullptr) {
    
  }

  ~Stream() {
    if (chunk_current_ != nullptr) {
      delete chunk_current_;
      chunk_current_ = nullptr;
    }
  }

 private:
  ByteArray* byte_array_;
  ByteArray* chunk_current_;
};


class ByteArrayCommon: public ByteArray {
 public:
  ByteArrayCommon()
      : data_(nullptr),
        chunk_(nullptr),
        size_(0),
        size_compressed_(0),
        off_(0),
        crc32_value_(0)
  {
  }
  virtual ~ByteArrayCommon() {
    if (chunk_ != nullptr) delete chunk_;
    chunk_ = nullptr;
  }
  virtual char* data() { return data_ + off_; }
  virtual char* data_const() const { return data_ + off_; }
  virtual uint64_t size() { return size_ - off_; }
  virtual uint64_t size_const() const { return size_ - off_; }
  virtual uint64_t size_compressed() { return size_compressed_ - off_; }
  virtual uint64_t size_compressed_const() const { return size_compressed_ - off_; }
  virtual uint32_t checksum() const { return crc32_value_; }
  virtual uint64_t offset() const { return off_; }
  virtual bool is_compressed() { return size_compressed_ > 0; }
  virtual void SetSizes(uint64_t size, uint64_t size_compressed) {
    size_ = size;
    size_compressed_ = size_compressed;
  }

  virtual bool StartsWith(const char *substr, int n) {
    return (n <= size_ && strncmp(data_, substr, n) == 0);
  }

  virtual void set_offset(int off) {
    off_ = off;
  }

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) = 0;
  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) = 0;

  /*
  char& operator[](std::size_t index) {
    return data_[index];
  };

  char operator[](std::size_t index) const {
    return data_[index];
  };
  */

  virtual std::string ToString() {
    return std::string(data(), size());
  }

  virtual void SetSizeCompressed(uint64_t s) { size_compressed_ = s; }
  virtual void SetCRC32(uint64_t c) { crc32_value_ = c; }

  bool IsStreamingRequired() { return false; }

  Stream* GetNewStream(uint64_t advised_chunk_size) {
    return nullptr;
  }

  // Streaming API - START

  void EnableChecksumVerification() {
    is_enabled_checksum_verification_ = true;
  }

  void SetInitialCRC32(uint32_t c32) {
    log::debug("SetInitialCRC32()", "Initial CRC32 0x%08" PRIx64 "\n", c32);
    if (is_enabled_checksum_verification_) {
      initial_crc32_ = c32;
      crc32_.put(c32); 
    }
  }

  // TODO: I think there is a bug here -- some subclass may later try to
  // call delete on data_, which will fail if it was shifted.
  void SetOffset(uint64_t offset, uint64_t size) {
    offset_ = offset;
    data_ = data_ + offset;
    size_ = size;
  }

  
  Status status_; 
  ByteArray* chunk_;
  bool is_valid_stream_;
  
  virtual void Begin() {
    if (is_enabled_checksum_verification_) {
      crc32_.ResetThreadLocalStorage();
      crc32_.put(initial_crc32_); 
    }
    if (chunk_ != nullptr) delete chunk_;
    chunk_ = nullptr;
    is_valid_stream_ = true;
    is_compression_disabled_ = false;
    offset_output_ = 0;
    compressor_.ResetThreadLocalStorage();
    Next();
    status_ = Status::IOError("Steam is unfinished");
  }

  virtual bool IsValid() {
    return is_valid_stream_;
  }

  virtual Status GetStatus() {
    return status_; 
  }

  // Careful here: if the call to Next() is the first one, i.e. the one in
  // Begin(), then is_valid_stream_ must not be set to false yet, otherwise
  // the for-loops of type for(Begin(); IsValid(); Next()) would never run,
  // as IsValid() would prevent the first iteration to start.
  virtual bool Next() {
    if (is_compressed() && !is_compression_disabled_) {

      if (compressor_.IsUncompressionDone(size_compressed_)) {
        is_valid_stream_ = false;
        if (   !is_enabled_checksum_verification_
            || crc32_.get() == crc32_value_) {
          log::debug("SharedMmappedByteArray::Next()", "Good CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
          status_ = Status::OK();
        } else {
          log::debug("SharedMmappedByteArray::Next()", "Bad CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
          status_ = Status::IOError("Invalid checksum.");
        }
        return false;
      }

      if (compressor_.HasFrameHeaderDisabledCompression(data_ + offset_output_)) {
        log::debug("SharedMmappedByteArray::Next()", "Finds that compression is disabled\n");
        is_compression_disabled_ = true;
        if (is_enabled_checksum_verification_) {
          crc32_.stream(data_ + offset_output_, compressor_.size_frame_header());
        }
        offset_output_ += compressor_.size_frame_header();
      }

      if (!is_compression_disabled_) {
        char *frame;
        uint64_t size_frame;

        char *data_out;
        uint64_t size_out;

        log::trace("SharedMmappedByteArray::Next()", "before uncompress");
        Status s = compressor_.Uncompress(data_,
                                          size_compressed_,
                                          &data_out,
                                          &size_out,
                                          &frame,
                                          &size_frame);
        offset_output_ += size_frame;

        if (chunk_ != nullptr) delete chunk_;
        //chunk_ = new SharedAllocatedByteArray(data_out, size_out);
        chunk_ = NewByteArrayChunk(data_out, size_out);

        if (s.IsDone()) {
          is_valid_stream_ = false;
          status_ = Status::OK();
        } else if (s.IsOK()) {
          if (is_enabled_checksum_verification_) {
            crc32_.stream(frame, size_frame);
          }
        } else {
          is_valid_stream_ = false;
          status_ = s;
        }
      }
    }

    if (!is_compressed() || is_compression_disabled_) {
      uint64_t size_left;
      if (is_compressed() && is_compression_disabled_) {
        size_left = size_compressed_;
      } else {
        size_left = size_;
      }

      if (offset_output_ == size_left) {
        is_valid_stream_ = false;
        status_ = Status::OK();
        return false;
      }

      char* data_left = data_ + offset_output_;

      size_t step = 1024*1024;
      size_t size_current = offset_output_ + step < size_left ? step : size_left - offset_output_;
      if (is_enabled_checksum_verification_) {
        crc32_.stream(data_left, size_current);
      }

      auto chunk = NewByteArrayClone(offset_output_, size_current);

      if (chunk_ != nullptr) delete chunk_;
      chunk_ = chunk;
      offset_output_ += size_current;
      status_ = Status::Done();
    }
    return true;
  }
  // Streaming API - END

  virtual ByteArray* GetChunk() {
    return chunk_;
  }

  CompressorLZ4 compressor_;
  uint32_t initial_crc32_;
  CRC32 crc32_;
  uint64_t offset_;
  uint64_t offset_output_;
  bool is_compression_disabled_;
  bool is_enabled_checksum_verification_;





  char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
  uint64_t off_;
  uint32_t crc32_value_;
};



class SimpleByteArray: public ByteArrayCommon {
 public:
  SimpleByteArray(const char* data_in, uint64_t size_in) {
    data_ = const_cast<char*>(data_in);
    size_ = size_in;
  }

  void AddOffset(int offset) {
    data_ += offset;
    size_ -= offset;
  }

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) {
    return new SimpleByteArray(data_out, size_out);
  }

  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) {
    return new SimpleByteArray(data_ + offset, size);
  }

  virtual ~SimpleByteArray() {
    //log::trace("SimpleByteArray::dtor()", "");
  }
};


// Like a smart pointer but for Byte Arrays
class SmartByteArray: public ByteArrayCommon {
 public:
  SmartByteArray(ByteArray* ba, const char* data_in, uint64_t size_in) {
    ba_ = ba;
    data_ = const_cast<char*>(data_in);
    size_ = size_in;
  }

  virtual ~SmartByteArray() {
    //log::trace("SmartByteArray::dtor()", "");
    delete ba_;
  }

  void AddOffset(int offset) {
    data_ += offset;
    size_ -= offset;
  }

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) {
    return ba_->NewByteArrayChunk(data_out, size_out);
  }

  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) {
    return ba_->NewByteArrayClone(offset, size);
  }


 private:
  ByteArray* ba_;

};


class AllocatedByteArray: public ByteArrayCommon {
 public:
  AllocatedByteArray(const char* data_in, uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_];
    memcpy(data_, data_in, size_);
  }

  AllocatedByteArray(uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_+1];
  }

  virtual ~AllocatedByteArray() {
    delete[] data_;
  }

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) {
    return new AllocatedByteArray(data_out, size_out);
  }

  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) {
    uint64_t size_max = std::max(size_, size_compressed_);
    AllocatedByteArray* ba = new AllocatedByteArray(data_, size_max);
    ba->size_ = size_;
    ba->SetOffset(offset, size);
    ba->off_ = this->offset();
    ba->crc32_value_ = checksum();
    ba->size_compressed_ = size_compressed();
    return ba;
  }
};


class SharedAllocatedByteArray: public ByteArrayCommon {
 public:
  SharedAllocatedByteArray() {}

  SharedAllocatedByteArray(char *data, uint64_t size_in) {
    data_allocated_ = std::shared_ptr<char>(data, [](char *p) { delete[] p; });
    data_ = data_allocated_.get();
    size_ = size_in;
  }

  SharedAllocatedByteArray(uint64_t size_in) {
    data_allocated_ = std::shared_ptr<char>(new char[size_in], [](char *p) { delete[] p; });
    data_ = data_allocated_.get();
    size_ = size_in;
  }

  virtual ~SharedAllocatedByteArray() {
    //log::trace("SharedAllocatedByteArray::dtor()", "");
  }

  void AddSize(int add) {
    size_ += add; 
  }

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) {
    return new AllocatedByteArray(data_out, size_out);
  }

  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) {
    SharedAllocatedByteArray* ba = new SharedAllocatedByteArray();
    *ba = *this;
    ba->SetOffset(offset, size);
    ba->chunk_ = nullptr;
    return ba;
  }

 private:
  std::shared_ptr<char> data_allocated_;
  uint64_t offset_;

};







// TODO: move to file.h
class Mmap {
 public:
  Mmap(std::string filepath, int64_t filesize)
      : filepath_(filepath),
        filesize_(filesize),
        is_valid_(false) {
    if ((fd_ = open(filepath.c_str(), O_RDONLY)) < 0) {
      log::emerg("Mmap()::ctor()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return;
    }

    log::trace("Mmap::ctor()", "open file: ok");

    datafile_ = static_cast<char*>(mmap(0,
                                       filesize, 
                                       PROT_READ,
                                       MAP_SHARED,
                                       fd_,
                                       0));
    if (datafile_ == MAP_FAILED) {
      log::emerg("Could not mmap() file [%s]: %s", filepath.c_str(), strerror(errno));
      return;
    }

    is_valid_ = true;
  }

  virtual ~Mmap() {
    Close();
  }

  void Close() {
    if (datafile_ != nullptr) {
      munmap(datafile_, filesize_);
      close(fd_);
      datafile_ = nullptr;
      log::debug("Mmap::~Mmap()", "released mmap on file: [%s]", filepath_.c_str());
    }
  }

  char* datafile() { return datafile_; }
  int64_t filesize() { return filesize_; }
  bool is_valid_;
  bool is_valid() { return is_valid_; }

  int fd_;
  int64_t filesize_;
  char *datafile_;

  // For debugging
  const char* filepath() const { return filepath_.c_str(); }
  std::string filepath_;
};


class SharedMmappedByteArray: public ByteArrayCommon {
 // TODO: Does the checksum and compressor here really need to be store in a
 // thread local storage? It would be way simpler to have this state held within
 // the ByteArray object.
 public:
  SharedMmappedByteArray() {}
  SharedMmappedByteArray(std::string filepath, int64_t filesize) {
    mmap_ = std::shared_ptr<Mmap>(new Mmap(filepath, filesize));
    data_ = mmap_->datafile();
    size_ = 0;
    is_compression_disabled_ = false;
    is_enabled_checksum_verification_ = false;
    offset_output_ = 0;
    compressor_.ResetThreadLocalStorage();
    crc32_.ResetThreadLocalStorage();
  }

  SharedMmappedByteArray(char *data, uint64_t size) {
    data_ = data;
    size_ = size;
    compressor_.ResetThreadLocalStorage();
    crc32_.ResetThreadLocalStorage();
  }

  virtual ~SharedMmappedByteArray() {}

  virtual ByteArray* NewByteArrayChunk(char* data_out, uint64_t size_out) {
    return new AllocatedByteArray(data_out, size_out);
  }

  virtual ByteArray* NewByteArrayClone(uint64_t offset, uint64_t size) {
    SharedMmappedByteArray* ba = new SharedMmappedByteArray();
    *ba = *this;
    ba->chunk_ = nullptr;
    ba->SetOffset(offset, size);
    return ba;
  }

  void AddSize(int add) {
    size_ += add; 
  }
  char* datafile() { return mmap_->datafile(); };
  std::shared_ptr<Mmap> mmap_;


};



}

#endif // KINGDB_BYTE_ARRAY_H_
