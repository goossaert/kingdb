// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMMON_H_
#define KINGDB_COMMON_H_

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "logger.h"
#include <spawn.h>

namespace kdb {

enum class OrderType { Put, Remove };

struct Order {
  OrderType type;
  const char *key;
  uint64_t size_key;
  const char *chunk;
  uint64_t size_chunk;
  uint64_t offset_chunk;
  uint64_t size_value;
  char * buffer_to_delete;
};


struct Entry {
  uint32_t action_type;
  uint64_t size_key;
  uint64_t size_value;
  uint64_t hash;
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



class Value {
 public:
  virtual ~Value() {}
  char *data;
  uint64_t size;

};




class ValueMmap: public Value {
 public:
  ValueMmap(std::string filepath, int filesize, uint64_t offset) {
    filename_debug_ = filepath;
    filesize_ = filesize;
    if ((fd_ = open(filepath.c_str(), O_RDONLY)) < 0) {
      LOG_EMERG("ValueMmap::ctor()", "Could not open file [%s]", filepath.c_str());
      //return Status::IOError(msg, strerror(errno));
    }

    LOG_TRACE("ValueMmap::ctor()", "Open file OK: [%s]", filepath.c_str());

    datafile_ = static_cast<char*>(mmap(0,
                                       filesize, 
                                       PROT_READ,
                                       MAP_SHARED,
                                       fd_,
                                       0));
    if (datafile_ == MAP_FAILED) {
      //return Status::IOError("Could not mmap() file", strerror(errno));
      LOG_EMERG("ValueMmap::ctor()", "Could not mmap() file: [%s] - %s", filepath.c_str(), strerror(errno));
      exit(-1);
    }

    struct Entry* entry = reinterpret_cast<struct Entry*>(datafile_ + offset);
    //LOG_EMERG("ValueMmap::ctor()", "file:[%s] size_key:%llu size_value:%llu, offset:%llu filesize:%llu datafile_:[%p]", filepath.c_str(), entry->size_key, entry->size_value, offset, filesize, datafile_);
    size = entry->size_value;
    if (offset + sizeof(struct Entry) + entry->size_key + entry->size_key > filesize) {
      LOG_EMERG("ValueMmap::ctor()", "ERROR: read out of the allowed memory space\n"); 
    }
    data = datafile_ + offset + sizeof(struct Entry) + entry->size_key;
  }

  virtual ~ValueMmap() {
    munmap(datafile_, filesize_);
    close(fd_);
  }
 private:
  int fd_;
  int filesize_;
  char *datafile_;
  std::string filename_debug_;
};


class ValueAllocated: public Value {
 public:
  ValueAllocated(const char* data_in, uint64_t size_in) {
    size = size_in;
    data = new char[size+1];
    strncpy(data, data_in, size);
    data[size] = '\0';
  }
  virtual ~ValueAllocated() {
    delete[] data;
  }
};




}


#endif // KINGDB_COMMON_H_
