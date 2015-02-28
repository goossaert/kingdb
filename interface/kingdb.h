// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

#include "util/debug.h"

#include <assert.h>
#include <thread>
#include <string>
#include <memory>
#include <sys/file.h>
#include <cstdint>
#include <inttypes.h>
#include <limits>

#include "interface/interface.h"
#include "cache/write_buffer.h"
#include "storage/storage_engine.h"
#include "storage/format.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/options.h"
#include "util/file.h"
#include "interface/iterator.h"
#include "interface/snapshot.h"
#include "interface/multipart.h"
#include "thread/threadstorage.h"

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
 
    FileUtil::increase_limit_open_files();

    Hash* hash = MakeHash(db_options_.hash);
    uint64_t max_size_hash = hash->MaxInputSize();
    delete hash;

    if (db_options_.storage__maximum_chunk_size > std::numeric_limits<int32_t>::max()) {
      return Status::IOError("db.storage.maximum-chunk-size cannot be greater than max int32. Fix your options.");
    }

    if (db_options_.storage__maximum_chunk_size >= db_options_.storage__hstable_size) {
      return Status::IOError("The maximum size of a chunk cannot be larger than the minimum size of a large file (db.storage.maximum-chunk-size >= db.storage.hstable-size). Fix your options.");
    }

    if (db_options_.storage__maximum_chunk_size > max_size_hash) {
      return Status::IOError("db.storage.maximum-chunk-size cannot be greater than the maximum input size of the hash function you chose. Fix your options.");
    }

    if (   db_options_.compression.type != kNoCompression
        && db_options_.storage__maximum_chunk_size > compressor_.MaxInputSize()) {
      return Status::IOError("db.storage.maximum-chunk-size cannot be greater than the maximum input size of the compression function you chose. Fix your options.");
    }

    std::unique_lock<std::mutex> lock(mutex_close_);
    if (!is_closed_) return Status::IOError("The database is already open");

    Status s;
    struct stat info;
    bool db_exists = (stat(dbname_.c_str(), &info) == 0);

    if(db_exists && !(info.st_mode & S_IFDIR)) {
      return Status::IOError("A file with same name as the database already exists and is not a directory. Delete or rename this file to continue.", dbname_.c_str());
    }

    if (   db_exists
        && db_options_.error_if_exists) {
      return Status::IOError("Could not create database directory", strerror(errno));
    }

    if (   !db_exists
        && db_options_.create_if_missing
        && mkdir(dbname_.c_str(), 0755) < 0) {
      return Status::IOError("Could not create database directory", strerror(errno));
    }

    std::string filepath_dboptions = DatabaseOptions::GetPath(dbname_);
    if (stat(filepath_dboptions.c_str(), &info) == 0) {
      // If there is a db_options file, try loading it
      log::trace("KingDB::Open()", "Loading db_option file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_RDONLY, 0644)) < 0) {
        log::emerg("KingDB::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }

      int ret = flock(fd_dboptions_, LOCK_EX | LOCK_NB);
      if (ret == EWOULDBLOCK || ret < 0) {
        close(fd_dboptions_);
        return Status::IOError("Could not acquire the global database lock: the database was already opened by another process"); 
      }

      Mmap mmap(filepath_dboptions, info.st_size);
      if (!mmap.is_valid()) return Status::IOError("Mmap() constructor failed");
      s = DatabaseOptionEncoder::DecodeFrom(mmap.datafile(), mmap.filesize(), &db_options_);
      if (!s.IsOK()) return s;
    } else {
      // If there is no db_options file, write it
      log::trace("KingDB::Open()", "Writing db_option file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
        log::emerg("KingDB::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }
      char buffer[DatabaseOptionEncoder::GetFixedSize()];
      DatabaseOptionEncoder::EncodeTo(&db_options_, buffer);
      if (write(fd_dboptions_, buffer, DatabaseOptionEncoder::GetFixedSize()) < 0) {
        close(fd_dboptions_);
        return Status::IOError("Could not write 'db_options' file", strerror(errno));
      }
    }

    em_ = new EventManager();
    wb_ = new WriteBuffer(db_options_, em_);
    se_ = new StorageEngine(db_options_, em_, dbname_);
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
    delete em_;
  }

  // TODO: make sure that if an entry cannot be returned because memory cannot
  // be allocated, a proper error message is returned -- same for the Iterator
  // and Snapshot

  virtual Status Get(ReadOptions& read_options, ByteArray& key, ByteArray* value_out) override;

  virtual Status Get(ReadOptions& read_options, ByteArray& key, std::string* value_out) {
    return Interface::Get(read_options, key, value_out);
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, ByteArray* value_out) {
    return Interface::Get(read_options, key, value_out);
  }

  virtual Status Get(ReadOptions& read_options, std::string& key, std::string* value_out) {
    return Interface::Get(read_options, key, value_out);
  }

  virtual Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& value) override;

  virtual Status Put(WriteOptions& write_options, ByteArray& key, std::string& chunk) {
    return Interface::Put(write_options, key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, ByteArray& chunk) {
    return Interface::Put(write_options, key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, std::string& key, std::string& chunk) {
    return Interface::Put(write_options, key, chunk);
  }

  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray& key,
                          ByteArray& chunk,
                          uint64_t offset_chunk, // TODO: could the offset be handled by the method itself?
                          uint64_t size_value) override;
  virtual Status Delete(WriteOptions& write_options, ByteArray& key) override;
  virtual Snapshot NewSnapshot();
  virtual Iterator NewIterator(ReadOptions& read_options) override;

  MultipartReader NewMultipartReader(ReadOptions& read_options, ByteArray& key) {
    ByteArray value;
    Status s = Get(read_options, key, &value, true);
    if (!s.IsOK()) {
      return MultipartReader(s);
    } else {
      return MultipartReader(read_options, value);
    }
  }


  MultipartWriter NewMultipartWriter(WriteOptions& write_options, ByteArray& key, uint64_t size_value_total) {
    return MultipartWriter(this, write_options, key, size_value_total);
  }



 private:
  Interface* NewSnapshotPointer();
  Status Get(ReadOptions& read_options,
             ByteArray& key,
             ByteArray* value_out,
             bool want_raw_data);

  Status PutChunkValidSize(WriteOptions& write_options,
                           ByteArray& key,
                           ByteArray& chunk,
                           uint64_t offset_chunk,
                           uint64_t size_value);

  kdb::DatabaseOptions db_options_;
  std::string dbname_;
  kdb::WriteBuffer *wb_;
  kdb::StorageEngine *se_;
  kdb::EventManager *em_;
  kdb::CompressorLZ4 compressor_;
  kdb::CRC32 crc32_;
  ThreadStorage ts_compression_enabled_;
  ThreadStorage ts_offset_;
  bool is_closed_;
  int fd_dboptions_;
  std::mutex mutex_close_;
};

} // namespace kdb

#endif // KINGDB_INTERFACE_MAIN_H_
