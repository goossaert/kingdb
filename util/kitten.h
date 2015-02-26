// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_KITTEN_H_
#define KINGDB_KITTEN_H_

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
#include "algorithm/compressor.h"
#include "algorithm/crc32c.h"


namespace kdb {

// TODO: move to file.h
class KittenMmap {
 public:
  KittenMmap(std::string filepath, int64_t filesize)
      : filepath_(filepath),
        filesize_(filesize),
        is_valid_(false) {
    if ((fd_ = open(filepath.c_str(), O_RDONLY)) < 0) {
      log::emerg("KittenMmap()::ctor()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return;
    }

    log::trace("KittenMmap::ctor()", "open file: ok");

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

  virtual ~KittenMmap() {
    Close();
  }

  void Close() {
    if (datafile_ != nullptr) {
      munmap(datafile_, filesize_);
      close(fd_);
      datafile_ = nullptr;
      log::debug("KittenMmap::~KittenMmap()", "released mmap on file: [%s]", filepath_.c_str());
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




class KittenResource {
 public:
  KittenResource() {}
  virtual ~KittenResource() {}
  virtual char* data() = 0;
  virtual const char* data_const() = 0;
  virtual uint64_t size() = 0;
  virtual const uint64_t size_const() = 0;
  virtual uint64_t size_compressed() = 0;
  virtual uint64_t const size_compressed_const() = 0;
};

class MmappedKittenResource: public KittenResource {
 friend class Kitten;
 public:
  virtual ~MmappedKittenResource() {
    //fprintf(stderr, "MmappedKittenResource::dtor()\n");
  }

  virtual char* data() { return data_; }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  MmappedKittenResource(std::string& filepath, uint64_t filesize)
    : data_(nullptr),
      size_(0),
      size_compressed_(0),
      mmap_(filepath, filesize) {
    if (mmap_.is_valid()) {
      data_ = mmap_.datafile(); 
      size_ = mmap_.filesize(); 
    }
    //fprintf(stderr, "MmappedKittenResource::ctor()\n"); 
  }

  KittenMmap mmap_;
  char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};


class AllocatedKittenResource: public KittenResource {
 friend class Kitten;
 public:
  virtual ~AllocatedKittenResource() {
    //fprintf(stderr, "AllocatedKittenResource::dtor()\n");
    delete[] data_;
  }

  virtual char* data() { return data_; }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  AllocatedKittenResource(char *data, uint64_t size, bool deep_copy)
    : data_(nullptr),
      size_(0),
      size_compressed_(0) {
    if (deep_copy) {
      size_ = size;
      data_ = new char[size_];
      memcpy(data_, data, size_);
    } else {
      size_ = size;
      data_ = data;
    }
    //fprintf(stderr, "AllocatedKittenResource::ctor()\n"); 
  }

  AllocatedKittenResource(uint64_t size)
    : data_(nullptr),
      size_(0),
      size_compressed_(0) {
    size_ = size;
    data_ = new char[size_];
  }

  char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};


class PointerKittenResource: public KittenResource {
 friend class Kitten;
 public:
  virtual ~PointerKittenResource() {}

  virtual char* data() { return const_cast<char*>(data_); }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  PointerKittenResource(const char *data, uint64_t size)
    : size_(size),
      size_compressed_(0),
      data_(data) {
  }

  const char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};





class Kitten {
 // TODO: what is happenning when a Kitten is assigned to another Kitten?
 friend class MultipartReader;
 friend class StorageEngine;
 friend class KingDB;
 friend class WriteBuffer;
 friend class BasicIterator;
 friend class NetworkTask;
 public:
  Kitten()
    : size_(0),
      size_compressed_(0),
      offset_(0),
      checksum_(0),
      checksum_initial_(0) {
  }

  virtual ~Kitten() {
  }

  virtual char* data() { return resource_->data() + offset_; }
  virtual const char* data_const() const { return resource_->data_const() + offset_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() const { return size_; }

  virtual std::string ToString() {
    return std::string(data(), size());
  }

  static Kitten NewShallowCopyKitten(char* data, uint64_t size) {
    Kitten kitten;
    kitten.resource_ = std::shared_ptr<KittenResource>(new AllocatedKittenResource(data, size, false));
    kitten.size_ = size;
    return kitten;
  }

  static Kitten NewDeepCopyKitten(const char* data, uint64_t size) {
    char* data_non_const = const_cast<char*>(data);
    Kitten kitten;
    kitten.resource_ = std::shared_ptr<KittenResource>(new AllocatedKittenResource(data_non_const, size, true));
    kitten.size_ = size;
    return kitten;
  }

  static Kitten NewDeepCopyKitten(std::string& str) {
    return NewDeepCopyKitten(str.c_str(), str.size());
  }

  static Kitten NewAllocatedMemoryKitten(uint64_t size) {
    Kitten kitten;
    kitten.resource_ = std::shared_ptr<KittenResource>(new AllocatedKittenResource(size));
    kitten.size_ = size;
    return kitten;
  }

  static Kitten NewMmappedKitten(std::string& filepath, uint64_t filesize) {
    Kitten kitten;
    kitten.resource_ = std::shared_ptr<KittenResource>(new MmappedKittenResource(filepath, filesize));
    kitten.size_ = filesize;
    return kitten;
  }

  static Kitten NewReferenceKitten(Kitten& kitten_in) {
    // TODO: make this the =operator()
    Kitten kitten = kitten_in;
    return kitten;
  }

  static Kitten NewPointerKitten(const char* data, uint64_t size) {
    Kitten kitten;
    kitten.resource_ = std::shared_ptr<KittenResource>(new PointerKittenResource(data, size));
    kitten.size_ = size;
    return kitten;
  }

  static Kitten NewEmptyKitten() {
    return Kitten();
  }

  bool operator ==(const Kitten &right) const {
    return (   size_const() == right.size_const()
            && memcmp(data_const(), right.data_const(), size_const()) == 0);
  }

 private:
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual uint64_t size_compressed_const() const { return size_compressed_; }
  virtual void set_size(uint64_t s) { size_ = s; }
  virtual void set_size_compressed(uint64_t s) { size_compressed_ = s; }
  virtual uint64_t is_compressed() { return (size_compressed_ != 0); }
  virtual void set_offset(uint64_t o) { offset_ = o; }
  virtual void increment_offset(uint64_t inc) { offset_ += inc; }

  virtual uint32_t checksum() { return checksum_; }
  virtual uint32_t checksum_initial() { return checksum_initial_; }
  virtual void set_checksum(uint32_t c) { checksum_ = c; }
  virtual void set_checksum_initial(uint32_t c) { checksum_initial_ = c; }

  std::shared_ptr<KittenResource> resource_;
  uint64_t size_;
  uint64_t size_compressed_;
  uint64_t offset_;

  uint32_t checksum_; // checksum for value_;
  uint32_t checksum_initial_; // initial checksum for value_ 
};

} // namespace kdb

#endif // KINGDB_BYTE_ARRAY_H_
