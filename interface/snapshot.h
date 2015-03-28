// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_SNAPSHOT_MAIN_H_
#define KINGDB_SNAPSHOT_MAIN_H_

#include <string>

#include "util/status.h"
#include "interface/iterator.h"
#include "interface/kingdb.h"
#include "interface/multipart.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/options.h"

namespace kdb {

class Snapshot: public KingDB {
 public:
  Snapshot()
      : se_live_(nullptr),
        se_readonly_(nullptr),
        fileids_iterator_(nullptr),
        is_closed_(true) {
  }

  Snapshot(const DatabaseOptions& db_options,
           const std::string dbname,
           StorageEngine *se_live,
           StorageEngine *se_readonly,
           std::vector<uint32_t>* fileids_iterator,
           uint32_t snapshot_id)
      : db_options_(db_options),
        dbname_(dbname),
        se_live_(se_live),
        se_readonly_(se_readonly),
        snapshot_id_(snapshot_id),
        fileids_iterator_(fileids_iterator),
        is_closed_(false) {
  }

  Snapshot(Snapshot&& s)
      : mutex_close_() {
    this->db_options_ = s.db_options_;
    this->dbname_ = s.dbname_;
    this->se_live_ = s.se_live_;
    this->se_readonly_ = s.se_readonly_;
    this->snapshot_id_ = s.snapshot_id_;
    this->fileids_iterator_ = s.fileids_iterator_;
    this->is_closed_ = s.is_closed_;
    s.fileids_iterator_ = nullptr;
    s.se_readonly_ = nullptr;
  }


  virtual ~Snapshot() {
    log::trace("Snapshot::dtor()", "");
    Close();
  }

  virtual Status Open() override {
    return Status::OK(); 
  }

  virtual void Close() override {
    log::trace("Snapshot::Close()", "start");
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    is_closed_ = true;
    delete fileids_iterator_;
    se_live_->ReleaseSnapshot(snapshot_id_);
    delete se_readonly_;
    log::trace("Snapshot::Close()", "end");
  }

  virtual Status Get(ReadOptions& read_options, ByteArray& key, ByteArray* value_out) override {
    Status s = se_readonly_->Get(read_options, key, value_out);
    if (s.IsNotFound()) {
      log::trace("Snapshot::Get()", "not found in storage engine");
      return s;
    } else if (s.IsOK()) {
      log::trace("Snapshot::Get()", "found in storage engine");
      return s;
    } else {
      log::trace("Snapshot::Get()", "unidentified error");
      return s;
    }

    return s;
  }

  virtual Status Put(WriteOptions& write_options, ByteArray& key, ByteArray& chunk) override {
    return Status::IOError("Not supported");
  }

  virtual Status PutPart(WriteOptions& write_options,
                          ByteArray& key,
                          ByteArray& chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) override {
    return Status::IOError("Not supported");
  }

  virtual Status Delete(WriteOptions& write_options, ByteArray& key) override {
    return Status::IOError("Not supported");
  }

  virtual Iterator NewIterator(ReadOptions& read_options) override {
    IteratorResource* ir = nullptr;
    uint64_t dbsize_uncompacted = se_readonly_->GetDbSizeUncompacted();
    if (dbsize_uncompacted > 0) {
      ir = new RegularIterator(read_options, se_readonly_, fileids_iterator_);
    } else {
      ir = new SequentialIterator(read_options, se_readonly_, fileids_iterator_);
    }
    Iterator it;
    it.SetIteratorResource(ir);
    return it;
  }

  virtual MultipartReader NewMultipartReader(ReadOptions& read_options, ByteArray& key) {
    ByteArray value;
    Status s = GetRaw(read_options, key, &value, true);
    if (!s.IsOK()) {
      return MultipartReader(s);
    } else {
      return MultipartReader(read_options, value);
    }
  }


  virtual void Flush() {}
  virtual void Compact() {}

 private:
  Status GetRaw(ReadOptions& read_options,
                ByteArray& key,
                ByteArray* value_out,
                bool want_raw_data) {
    // WARNING: code duplication with Database::GetRaw()
    if (is_closed_) return Status::IOError("The database is not open");
    log::trace("Database GetRaw()", "[%s]", key.ToString().c_str());
    Status s = se_readonly_->Get(read_options, key, value_out);
    if (s.IsNotFound()) {
      log::trace("Database GetRaw()", "not found in storage engine");
      return s;
    } else if (s.IsOK()) {
      log::trace("Database GetRaw()", "found in storage engine");
    } else {
      log::trace("Database GetRaw()", "unidentified error");
      return s;
    }

    // TODO-36: There is technical debt here:
    // 1. The uncompression should be able to proceed without having to call a
    //    Multipart Reader.
    // 2. The uncompression should be able to operate within a single buffer, and
    //    not have to copy data into intermediate buffers through the Multipart
    //    Reader as it is done here. Having intermediate buffers means that there
    //    is more data copy than necessary, thus more time wasted
    log::trace("Database GetRaw()", "Before Multipart - want_raw_data:%d value_out->is_compressed():%d", want_raw_data, value_out->is_compressed());
    if (want_raw_data == false && value_out->is_compressed()) {
      if (value_out->size() > db_options_.internal__size_multipart_required) {
        return Status::MultipartRequired();
      }
      char* buffer = new char[value_out->size()];
      uint64_t offset = 0;
      MultipartReader mp_reader(read_options, *value_out);
      for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
        ByteArray part;
        mp_reader.GetPart(&part);
        log::trace("Database GetRaw()", "Multipart loop size:%d [%s]", part.size(), part.ToString().c_str());
        memcpy(buffer + offset, part.data(), part.size());
        offset += part.size();
      }
      *value_out = NewShallowCopyByteArray(buffer, value_out->size());
    }

    return s;
  }

  kdb::DatabaseOptions db_options_;
  std::string dbname_;
  kdb::StorageEngine* se_live_;
  kdb::StorageEngine* se_readonly_;
  uint32_t snapshot_id_;
  std::vector<uint32_t>* fileids_iterator_;
  bool is_closed_;
  std::mutex mutex_close_;
};

} // end namespace kdb

#endif // KINGDB_SNAPSHOT_MAIN_H_
