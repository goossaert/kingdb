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

// TODO-1: most of the uses of ByteArray classes are pointers
//         => change that to use references whenever possible


class ByteArrayCommon: public ByteArray {
 public:
  ByteArrayCommon()
      : data_(nullptr),
        size_(0),
        size_compressed_(0),
        off_(0),
        crc32_value_(0)
  {
  }
  virtual ~ByteArrayCommon() {}
  virtual char* data() { return data_ + off_; }
  virtual char* data_const() const { return data_ + off_; }
  virtual uint64_t size() { return size_ - off_; }
  virtual uint64_t size_const() const { return size_ - off_; }
  virtual uint64_t size_compressed() { return size_compressed_ - off_; }
  virtual uint64_t size_compressed_const() const { return size_compressed_ - off_; }
  virtual bool is_compressed() { return size_compressed_ > 0; }

  virtual bool StartsWith(const char *substr, int n) {
    return (n <= size_ && strncmp(data_, substr, n) == 0);
  }

  virtual void set_offset(int off) {
    off_ = off;
  }

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

  virtual Status data_chunk(char **data, uint64_t *size) {
    *size = size_;
    *data = data_;
    return Status::Done();
  }

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

  void AddOffset(int offset) {
    data_ += offset;
    size_ -= offset;
  }

  virtual ~SmartByteArray() {
    //log::trace("SmartByteArray::dtor()", "");
    delete ba_;
  }

 private:
  ByteArray* ba_;

};



// TODO: move to file.h
class Mmap {
 public:
  Mmap(std::string filepath, int64_t filesize)
      : filepath_(filepath),
        filesize_(filesize),
        is_valid_(false) {
    if ((fd_ = open(filepath.c_str(), O_RDONLY)) < 0) {
      std::string msg = std::string("Count not open file [") + filepath + std::string("]");
      log::emerg("Mmap()::ctor()", "%s", msg.c_str());
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
      std::string message("Could not mmap() file: " + filepath);
      log::emerg(message.c_str(), strerror(errno));
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
  const char* filepath() const { return filepath_.c_str(); } // for debugging
  bool is_valid_;
  bool is_valid() { return is_valid_; }

  int fd_;
  int64_t filesize_;
  char *datafile_;
  std::string filepath_; // just for debugging
};


class SharedMmappedByteArray: public ByteArrayCommon {
 public:
  SharedMmappedByteArray() {}
  SharedMmappedByteArray(std::string filepath, int64_t filesize) {
    mmap_ = std::shared_ptr<Mmap>(new Mmap(filepath, filesize));
    data_ = mmap_->datafile();
    size_ = 0;
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

  void SetOffset(uint64_t offset, uint64_t size) {
    offset_ = offset;
    data_ = mmap_->datafile() + offset;
    size_ = size;
  }

  void AddSize(int add) {
    size_ += add; 
  }

  void SetInitialCRC32(uint32_t c32) {
    crc32_.put(c32); 
  }

  virtual Status data_chunk(char **data_out, uint64_t *size_out) {
    if (!is_compressed()) {
      // TODO: fix bug here -- if size_ is bigger than 2^31 - 1, crc32 will fail
      /*
      crc32_.stream(data_, size_);
      if (crc32_.get() != crc32_value_) {
        log::debug("SharedMmappedByteArray::data_chunk()", "Bad CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
        return Status::IOError("Bad CRC32");
      }
      */
      *data_out = data_;
      *size_out = size_;
      return Status::Done();
    }

    *data_out = nullptr;
    *size_out = 0;

    char *frame;
    uint64_t size_frame;

    log::trace("data_chunk()", "start");
    Status s = compressor_.Uncompress(data_,
                                      size_compressed_,
                                      data_out,
                                      size_out,
                                      &frame,
                                      &size_frame);

    if (s.IsDone()) {
      if (crc32_.get() == crc32_value_) {
        log::debug("SharedMmappedByteArray::data_chunk()", "Good CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
        return s;
      } else {
        log::debug("SharedMmappedByteArray::data_chunk()", "Bad CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
        return Status::IOError("Bad CRC32");
      }
    } else if (!s.IsOK()) {
      log::debug("SharedMmappedByteArray::data_chunk()", "Error CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", crc32_value_, crc32_.get());
      return s;
    }

    crc32_.stream(frame, size_frame);
    return Status::OK();
  }

  char* datafile() { return mmap_->datafile(); };

 private:
  CompressorLZ4 compressor_;
  CRC32 crc32_;
  std::shared_ptr<Mmap> mmap_;
  uint64_t offset_;
};


class AllocatedByteArray: public ByteArrayCommon {
 public:
  AllocatedByteArray(const char* data_in, uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_];
    strncpy(data_, data_in, size_);
  }

  AllocatedByteArray(uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_+1];
  }

  virtual ~AllocatedByteArray() {
    delete[] data_;
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

  void SetOffset(uint64_t offset, uint64_t size) {
    offset_ = offset;
    data_ = data_allocated_.get() + offset;
    size_ = size;
  }

  void AddSize(int add) {
    size_ += add; 
  }

 private:
  std::shared_ptr<char> data_allocated_;
  uint64_t offset_;

};



}

#endif // KINGDB_BYTE_ARRAY_H_
