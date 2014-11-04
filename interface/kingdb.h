// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

#include <assert.h>
#include <thread>
#include <string>
#include <memory>
#include <sys/file.h>

#include "interface/interface.h"
#include "cache/write_buffer.h"
#include "storage/storage_engine.h"
#include "storage/format.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/options.h"
#include "interface/iterator.h"
#include "interface/snapshot.h"

#include "algorithm/compressor.h"
#include "algorithm/crc32c.h"
#include "algorithm/endian.h"

namespace kdb {

// TODO: Add Flush() method?

class KingDB: public Interface {
 public:
  KingDB(const DatabaseOptions& db_options, const std::string dbname)
      : db_options_(db_options),
        dbname_(dbname),
        is_closed_(true)
  {
    // Word-swapped endianness is not supported
    assert(getEndianness() == kBytesLittleEndian || getEndianness() == kBytesBigEndian);
  }

  virtual ~KingDB() {
    Close();
  }
  
  virtual Status Open() override {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (!is_closed_) return Status::IOError("The database is already open");

    Status s;
    struct stat info;
    bool db_exists = (stat(dbname_.c_str(), &info) == 0);

    if (   db_exists
        && db_options_.error_if_exists) {
      return Status::IOError("Could not create database directory", strerror(errno));
    }

    if (   !db_exists
        && db_options_.create_if_missing
        && mkdir(dbname_.c_str(), 0755) < 0) {
      return Status::IOError("Could not create database directory", strerror(errno));
    }

    if(!(info.st_mode & S_IFDIR)) {
      return Status::IOError("A file with same name as the database already exists and is not a directory. Remove or rename this file to continue.", dbname_.c_str());
    }

    std::string filepath_dboptions = DatabaseOptions::GetPath(dbname_);
    if (stat(filepath_dboptions.c_str(), &info) == 0) {
      // If there is a db_options file, try loading it
      LOG_TRACE("KingDB::Open()", "Loading db_option file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_RDONLY, 0644)) < 0) {
        LOG_EMERG("KingDB::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }

      int ret = flock(fd_dboptions_, LOCK_EX | LOCK_NB);
      if (ret == EWOULDBLOCK) {
        close(fd_dboptions_);
        return Status::IOError("The database is already in use by another process"); 
      } else if (ret < 0) {
        close(fd_dboptions_);
        return Status::IOError("Unknown error when trying to acquire the global database lock");
      }

      Mmap mmap(filepath_dboptions, info.st_size);
      s = DatabaseOptionEncoder::DecodeFrom(mmap.datafile(), mmap.filesize(), &db_options_);
      if (!s.IsOK()) return s;
    } else {
      // If there is no db_options file, write it
      LOG_TRACE("KingDB::Open()", "Writing db_option file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
        LOG_EMERG("KingDB::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }
      char buffer[DatabaseOptionEncoder::GetFixedSize()];
      DatabaseOptionEncoder::EncodeTo(&db_options_, buffer);
      if (write(fd_dboptions_, buffer, DatabaseOptionEncoder::GetFixedSize()) < 0) {
        close(fd_dboptions_);
        return Status::IOError("Could not write 'db_options' file", strerror(errno));
      }
    }

    wb_ = new WriteBuffer(db_options_);
    se_ = new StorageEngine(db_options_, dbname_);
    is_closed_ = false;
    return Status::OK(); 
  }

  virtual void Close() override {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    flock(fd_dboptions_, LOCK_UN);
    close(fd_dboptions_);
    is_closed_ = true;
    wb_->Close();
    se_->Close();
    delete wb_;
    delete se_;
  }

  virtual Status Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) override;
  virtual Status Put(WriteOptions& write_options, ByteArray *key, ByteArray *chunk) override;
  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray *key,
                          ByteArray *chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) override;
  virtual Status Remove(WriteOptions& write_options, ByteArray *key) override;
  virtual Interface* NewSnapshot() override;
  virtual Iterator* NewIterator(ReadOptions& read_options) override { return nullptr; };

 private:
  // TODO-6: Make sure that if multiple threads are creating KingDB objects with
  //         the same database name, they would all refer to the same buffer
  //         manager and storage engine.
  kdb::DatabaseOptions db_options_;
  std::string dbname_;
  kdb::WriteBuffer *wb_;
  kdb::StorageEngine *se_;
  kdb::CompressorLZ4 compressor_;
  kdb::CRC32 crc32_;
  bool is_closed_;
  int fd_dboptions_;
  std::mutex mutex_close_;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_MAIN_H_
