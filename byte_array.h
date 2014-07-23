// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_BYTE_ARRAY_H_
#define KINGDB_BYTE_ARRAY_H_

#include <memory>
#include <string>
#include <string.h>

#include "logger.h"

namespace kdb {

class ByteArray {
 public:
  virtual ~ByteArray() {}
  char* data() { return data_; }
  uint64_t size() { return size_; }

  bool StartsWith(const char *substr, int n) {
    return (n <= size_ && strncmp(data_, substr, n) == 0);
  }

  /*
  char& operator[](std::size_t index) {
    return data_[index];
  };

  char operator[](std::size_t index) const {
    return data_[index];
  };
  */

  std::string ToString() {
    return std::string(data_, size_);
  }

  char *data_;
  uint64_t size_;
};


class Mmap {
 public:
  Mmap(std::string filepath, int filesize) {
    filepath_ = filepath;
    filesize_ = filesize;
    if ((fd_ = open(filepath.c_str(), O_RDONLY)) < 0) {
      std::string msg = std::string("Count not open file [") + filepath + std::string("]");
      LOG_EMERG("ByteArrayMmap()::ctor()", "%s", msg.c_str());
      //return Status::IOError(msg, strerror(errno));
    }

    LOG_TRACE("StorageEngine::GetEntry()", "open file: ok");

    datafile_ = static_cast<char*>(mmap(0,
                                       filesize, 
                                       PROT_READ,
                                       MAP_SHARED,
                                       fd_,
                                       0));
    if (datafile_ == MAP_FAILED) {
      //return Status::IOError("Could not mmap() file", strerror(errno));
      LOG_EMERG("Could not mmap() file: %s", strerror(errno));
      exit(-1);
    }
    
  }

  virtual ~Mmap() {
    munmap(datafile_, filesize_);
    close(fd_);
    LOG_DEBUG("Mmap::~Mmap()", "released mmap on file: [%s]", filepath_.c_str());
  }

  int fd_;
  int filesize_;
  char *datafile_;
  std::string filepath_; // just for debugging
};


class SharedMmappedByteArray: public ByteArray {
 public:
  SharedMmappedByteArray() {}
  SharedMmappedByteArray(std::string filepath, int filesize) {
    mmap_ = std::shared_ptr<Mmap>(new Mmap(filepath, filesize));
    data_ = mmap_->datafile_;
    size_ = 0;
  }

  void SetOffset(uint64_t offset, uint64_t size) {
    offset_ = offset;
    data_ = mmap_->datafile_ + offset;
    size_ = size;
  }

  char* datafile() { return mmap_->datafile_; };

 private:
  std::shared_ptr<Mmap> mmap_;
  uint64_t offset_;
};


class AllocatedByteArray: public ByteArray {
 public:
  AllocatedByteArray(const char* data_in, uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_+1];
    strncpy(data_, data_in, size_);
    data_[size_] = '\0';
  }

  AllocatedByteArray(uint64_t size_in) {
    size_ = size_in;
    data_ = new char[size_+1];
  }

  virtual ~AllocatedByteArray() {
    delete[] data_;
  }

  std::shared_ptr<char> data_allocated_;
};



class SharedAllocatedByteArray: public ByteArray {
 public:
  SharedAllocatedByteArray() {}

  SharedAllocatedByteArray(uint64_t size_in) {
    data_allocated_ = std::shared_ptr<char>(new char[size_in], [](char *p) { delete[] p; });
    data_ = data_allocated_.get();
    size_ = size_in;
  }

  void SetOffset(uint64_t offset, uint64_t size) {
    offset_ = offset;
    data_ = data_allocated_.get() + offset;
    size_ = size;
  }

  virtual ~SharedAllocatedByteArray() {
  }

 private:
  std::shared_ptr<char> data_allocated_;
  uint64_t offset_;

};

}


#endif // KINGDB_BYTE_ARRAY_H_

