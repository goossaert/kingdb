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
#include <cinttypes>
#include <limits>

#include "interface/kingdb.h"
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

class Database: public KingDB {
 public:
  Database(const DatabaseOptions& db_options, const std::string& dbname)
      : db_options_(db_options),
        dbname_(FixDatabaseName(dbname)),
        is_closed_(true)
  {
    // Word-swapped endianness is not supported
    assert(getEndianness() == kBytesLittleEndian || getEndianness() == kBytesBigEndian);
  }

  Database(const std::string& dbname)
      : dbname_(FixDatabaseName(dbname)),
        is_closed_(true)
  {
    // Word-swapped endianness is not supported
    assert(getEndianness() == kBytesLittleEndian || getEndianness() == kBytesBigEndian);
  }

  virtual ~Database() {
    Close();
  }

  std::string FixDatabaseName(const std::string& dbname) {
    // Allows both relative and absolute directory paths to work
    if (dbname.size() >= 1 && dbname[0] == '/') {
      return dbname; 
    } else if (dbname.size() >= 2 && dbname[0] == '.' && dbname[1] == '/') {
      return FileUtil::kingdb_getcwd() + "/" + dbname.substr(2);
    } else {
      return FileUtil::kingdb_getcwd() + "/" + dbname;
    }
  }
 
  virtual Status Open() override {
    FileUtil::increase_limit_open_files();

    Status s;
    struct stat info;
    bool db_exists = (stat(dbname_.c_str(), &info) == 0);

    if (db_exists && !(info.st_mode & S_IFDIR)) {
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
    bool db_options_exists = (stat(filepath_dboptions.c_str(), &info) == 0);
    Status status_dboptions;
    DatabaseOptions db_options_candidate;
    if (db_options_exists) {
      // If there is a db_options file, try loading it
      log::trace("Database::Open()", "Loading db_options file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_RDONLY, 0644)) < 0) {
        log::emerg("Database::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }

      int ret = flock(fd_dboptions_, LOCK_EX | LOCK_NB);
      if (ret == EWOULDBLOCK || ret < 0) {
        close(fd_dboptions_);
        return Status::IOError("Could not acquire the global database lock: the database was already opened by another process"); 
      }

      Mmap mmap(filepath_dboptions, info.st_size);
      if (!mmap.is_valid()) return Status::IOError("Mmap() constructor failed");
      status_dboptions = DatabaseOptionEncoder::DecodeFrom(mmap.datafile(), mmap.filesize(), &db_options_candidate);
      if (status_dboptions.IsOK()) db_options_ = db_options_candidate;
    }
    
    if (db_exists && (!db_options_exists || !status_dboptions.IsOK())) {
      // The database already existed, but no db_options file was found in the
      // database directory, or the db_options file was present but invalid,
      // thus it needs to be recovered.
      DatabaseOptions db_options_candidate;
      std::string prefix_compaction = StorageEngine::GetCompactionFilePrefix();
      Status s = HSTableManager::LoadDatabaseOptionsFromHSTables(dbname_,
                                                                  &db_options_candidate,
                                                                  prefix_compaction);
      if (s.IsOK()) db_options_ = db_options_candidate;
    }

    if (!db_exists || !db_options_exists || !status_dboptions.IsOK()) {
      // If there is no db_options file, or if it's invalid, write it
      log::trace("Database::Open()", "Writing db_options file");
      if ((fd_dboptions_ = open(filepath_dboptions.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
        log::emerg("Database::Open()", "Could not open file [%s]: %s", filepath_dboptions.c_str(), strerror(errno));
      }
      char buffer[DatabaseOptionEncoder::GetFixedSize()];
      DatabaseOptionEncoder::EncodeTo(&db_options_, buffer);
      if (write(fd_dboptions_, buffer, DatabaseOptionEncoder::GetFixedSize()) < 0) {
        close(fd_dboptions_);
        return Status::IOError("Could not write 'db_options' file", strerror(errno));
      }
    }

    Hash* hash = MakeHash(db_options_.hash);
    uint64_t max_size_hash = hash->MaxInputSize();
    delete hash;

    if (db_options_.storage__maximum_part_size > std::numeric_limits<int32_t>::max()) {
      return Status::IOError("db.storage.maximum-part-size cannot be greater than max int32. Fix your options.");
    }

    if (db_options_.storage__maximum_part_size >= db_options_.storage__hstable_size) {
      return Status::IOError("The maximum size of a chunk cannot be larger than the minimum size of a large file (db.storage.maximum-part-size >= db.storage.hstable-size). Fix your options.");
    }

    if (db_options_.storage__maximum_part_size > max_size_hash) {
      return Status::IOError("db.storage.maximum-part-size cannot be greater than the maximum input size of the hash function you chose. Fix your options.");
    }

    if (   db_options_.compression.type != kNoCompression
        && db_options_.storage__maximum_part_size > compressor_.MaxInputSize()) {
      return Status::IOError("db.storage.maximum-part-size cannot be greater than the maximum input size of the compression function you chose. Fix your options.");
    }

    std::unique_lock<std::mutex> lock(mutex_close_);
    if (!is_closed_) return Status::IOError("The database is already open");

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
    return KingDB::Get(read_options, key, value_out);
  }

  virtual Status Get(ReadOptions& read_options, const std::string& key, ByteArray* value_out) {
    return KingDB::Get(read_options, key, value_out);
  }

  virtual Status Get(ReadOptions& read_options, const std::string& key, std::string* value_out) {
    return KingDB::Get(read_options, key, value_out);
  }

  virtual Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& value) override;

  virtual Status Put(WriteOptions& write_options, ByteArray& key, const std::string& chunk) {
    return KingDB::Put(write_options, key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, const std::string& key, ByteArray& chunk) {
    return KingDB::Put(write_options, key, chunk);
  }

  virtual Status Put(WriteOptions& write_options, const std::string& key, const std::string& chunk) {
    return KingDB::Put(write_options, key, chunk);
  }

  virtual Status PutPart(WriteOptions& write_options,
                          ByteArray& key,
                          ByteArray& chunk,
                          uint64_t offset_chunk, // TODO: could the offset be handled by the method itself?
                          uint64_t size_value) override;

  virtual Status Delete(WriteOptions& write_options, ByteArray& key) override;

  virtual Status Delete(WriteOptions& write_options, const std::string& key) {
    return KingDB::Delete(write_options, key);
  }

  virtual Snapshot NewSnapshot();
  virtual Iterator NewIterator(ReadOptions& read_options) override;

  virtual MultipartReader NewMultipartReader(ReadOptions& read_options, ByteArray& key) {
    ByteArray value;
    Status s = GetRaw(read_options, key, &value, true);
    if (!s.IsOK()) {
      return MultipartReader(s);
    } else {
      return MultipartReader(read_options, value);
    }
  }

  virtual MultipartReader NewMultipartReader(ReadOptions& read_options, const std::string& key_str) {
    ByteArray key = NewDeepCopyByteArray(key_str);
    return NewMultipartReader(read_options, key);
  }

  MultipartWriter NewMultipartWriter(WriteOptions& write_options, ByteArray& key, uint64_t size_value_total) {
    return MultipartWriter(this, write_options, key, size_value_total);
  }

  MultipartWriter NewMultipartWriter(WriteOptions& write_options, const std::string& key_str, uint64_t size_value_total) {
    ByteArray key = NewDeepCopyByteArray(key_str);
    return MultipartWriter(this, write_options, key, size_value_total);
  }

  virtual void Flush();
  virtual void Compact();


 private:
  KingDB* NewSnapshotPointer();
  Status GetRaw(ReadOptions& read_options,
                ByteArray& key,
                ByteArray* value_out,
                bool want_raw_data);

  Status PutPartValidSize(WriteOptions& write_options,
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
