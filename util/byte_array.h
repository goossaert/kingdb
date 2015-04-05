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

#include "util/file.h"
#include "util/logger.h"
#include "util/options.h"
#include "util/filepool.h"


namespace kdb {

class ByteArrayResource {
 public:
  ByteArrayResource() {}
  virtual ~ByteArrayResource() {}
  virtual char* data() = 0;
  virtual const char* data_const() = 0;
  virtual uint64_t size() = 0;
  virtual const uint64_t size_const() = 0;
  virtual uint64_t size_compressed() = 0;
  virtual uint64_t const size_compressed_const() = 0;
};


class PooledByteArrayResource: public ByteArrayResource {
 friend class ByteArray;
 public:
  PooledByteArrayResource(std::shared_ptr<FileManager> file_manager, uint32_t fileid, const std::string& filepath, uint64_t filesize)
    : data_(nullptr),
      file_manager_(file_manager),
      size_(0),
      size_compressed_(0),
      fileid_(0) {
    Status s = file_manager_->GetFile(fileid, filepath, filesize, &file_resource_);
    if (s.IsOK()) {
      data_ = file_resource_.mmap;
      size_ = file_resource_.filesize;
      fileid_ = fileid;
    } else {
      fprintf(stderr, "invalid file resource: should never happen\n"); 
    }
  }

  virtual ~PooledByteArrayResource() {
    file_manager_->ReleaseFile(fileid_, file_resource_.filesize);
  }

  virtual char* data() { return data_; }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  char *data_;
  uint32_t fileid_;
  uint64_t size_;
  uint64_t size_compressed_;
  FileResource file_resource_;
  std::shared_ptr<FileManager> file_manager_;
};




class MmappedByteArrayResource: public ByteArrayResource {
 friend class ByteArray;
 public:
  MmappedByteArrayResource(const std::string& filepath, uint64_t filesize)
    : data_(nullptr),
      size_(0),
      size_compressed_(0),
      mmap_(filepath, filesize) {
    if (mmap_.is_valid()) {
      data_ = mmap_.datafile(); 
      size_ = mmap_.filesize(); 
    }
  }

  virtual ~MmappedByteArrayResource() {
  }

  virtual char* data() { return data_; }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  Mmap mmap_;
  char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};


class AllocatedByteArrayResource: public ByteArrayResource {
 friend class ByteArray;
 public:
  AllocatedByteArrayResource(char *data, uint64_t size, bool deep_copy)
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
  }

  AllocatedByteArrayResource(uint64_t size)
    : data_(nullptr),
      size_(0),
      size_compressed_(0) {
    size_ = size;
    data_ = new char[size_];
  }

  virtual ~AllocatedByteArrayResource() {
    delete[] data_;
  }

  virtual char* data() { return data_; }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }

 private:
  char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};


class PointerByteArrayResource: public ByteArrayResource {
 friend class ByteArray;
 public:
  PointerByteArrayResource(const char *data, uint64_t size)
    : size_(size),
      size_compressed_(0),
      data_(data) {
  }
  virtual ~PointerByteArrayResource() {}

  virtual char* data() { return const_cast<char*>(data_); }
  virtual const char* data_const() { return data_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() { return size_; }
  virtual uint64_t size_compressed() { return size_compressed_; }
  virtual const uint64_t size_compressed_const() { return size_compressed_; }
 
 private:
  const char *data_;
  uint64_t size_;
  uint64_t size_compressed_;
};


class ByteArray {
 // TODO: what is happenning when a ByteArray is assigned to another ByteArray?
 friend class MultipartReader;
 friend class StorageEngine;
 friend class Database;
 friend class Snapshot;
 friend class WriteBuffer;
 friend class RegularIterator;
 friend class SequentialIterator;
 friend class NetworkTask;
 friend class CompressorLZ4;
 public:
  ByteArray()
    : size_(0),
      size_compressed_(0),
      offset_(0),
      checksum_(0),
      checksum_initial_(0) {
  }

  virtual ~ByteArray() {
  }

  virtual char* data() { return resource_->data() + offset_; }
  virtual const char* data_const() const { return resource_->data_const() + offset_; }
  virtual uint64_t size() { return size_; }
  virtual const uint64_t size_const() const { return size_; }

  virtual std::string ToString() {
    return std::string(data(), size());
  }

  static ByteArray NewShallowCopyByteArray(char* data, uint64_t size) {
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<AllocatedByteArrayResource>(data, size, false);
    byte_array.size_ = size;
    return byte_array;
  }

  static ByteArray NewDeepCopyByteArray(const char* data, uint64_t size) {
    char* data_non_const = const_cast<char*>(data);
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<AllocatedByteArrayResource>(data_non_const, size, true);
    byte_array.size_ = size;
    return byte_array;
  }

  static ByteArray NewDeepCopyByteArray(const std::string& str) {
    return NewDeepCopyByteArray(str.c_str(), str.size());
  }

  static ByteArray NewMmappedByteArray(const std::string& filepath, uint64_t filesize) {
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<MmappedByteArrayResource>(filepath, filesize);
    byte_array.size_ = filesize;
    return byte_array;
  }

  static ByteArray NewPointerByteArray(const char* data, uint64_t size) {
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<PointerByteArrayResource>(data, size);
    byte_array.size_ = size;
    return byte_array;
  }

  bool operator ==(const ByteArray &right) const {
    return (   size_const() == right.size_const()
            && memcmp(data_const(), right.data_const(), size_const()) == 0);
  }

 private:

  static ByteArray NewAllocatedMemoryByteArray(uint64_t size) {
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<AllocatedByteArrayResource>(size);
    byte_array.size_ = size;
    return byte_array;
  }

  static ByteArray NewReferenceByteArray(ByteArray& byte_array_in) {
    // TODO: make this the =operator()
    ByteArray byte_array = byte_array_in;
    return byte_array;
  }

  static ByteArray NewPooledByteArray(std::shared_ptr<FileManager> file_manager, uint32_t fileid, const std::string& filepath, uint64_t filesize) {
    ByteArray byte_array;
    byte_array.resource_ = std::make_shared<PooledByteArrayResource>(file_manager, fileid, filepath, filesize);
    byte_array.size_ = filesize;
    return byte_array;
  }

  static ByteArray NewEmptyByteArray() {
    return ByteArray();
  }

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

  std::shared_ptr<ByteArrayResource> resource_;
  uint64_t size_;
  uint64_t size_compressed_;
  uint64_t offset_;

  uint32_t checksum_; // checksum for value_;
  uint32_t checksum_initial_; // initial checksum for value_ 
};


// ByteArray helpers
inline ByteArray NewShallowCopyByteArray(char* data, uint64_t size) {
  return ByteArray::NewShallowCopyByteArray(data, size);
}

inline ByteArray NewDeepCopyByteArray(const char* data, uint64_t size) {
  return ByteArray::NewDeepCopyByteArray(data, size);
}

inline ByteArray NewDeepCopyByteArray(const std::string& str) {
  return ByteArray::NewDeepCopyByteArray(str.c_str(), str.size());
}

inline ByteArray NewMmappedByteArray(const std::string& filepath, uint64_t filesize) {
  return ByteArray::NewMmappedByteArray(filepath, filesize);
}

inline ByteArray NewPointerByteArray(const char* data, uint64_t size) {
  return ByteArray::NewPointerByteArray(data, size);
}

} // namespace kdb

#endif // KINGDB_BYTE_ARRAY_H_
