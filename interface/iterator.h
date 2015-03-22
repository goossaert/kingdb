// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_ITERATOR_MAIN_H_
#define KINGDB_ITERATOR_MAIN_H_

#include <string>

#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/options.h"
#include "util/file.h"
#include "interface/kingdb.h"
#include "interface/multipart.h"
#include "storage/storage_engine.h"

namespace kdb {

class IteratorResource {
 public:
  virtual ~IteratorResource() {};
  virtual void Close() = 0;
  virtual void SetParentSnapshot(KingDB *snapshot) = 0;
  virtual void Begin() = 0;
  virtual bool IsValid() = 0;
  virtual bool Next() = 0;
  virtual ByteArray GetKey() = 0;
  virtual ByteArray GetValue() = 0;
  virtual MultipartReader GetMultipartValue() = 0;
  virtual Status GetStatus() = 0;
};



class RegularIterator: public IteratorResource {
 public:
  RegularIterator()
    : is_closed_(true),
      se_readonly_(nullptr),
      snapshot_(nullptr),
      status_(Status::IOError("Invalid iterator")) {
  }

  RegularIterator(ReadOptions& read_options,
           StorageEngine *se_readonly,
           std::vector<uint32_t>* fileids_iterator)
    : is_closed_(false),
      se_readonly_(se_readonly),
      read_options_(read_options),
      snapshot_(nullptr),
      fileids_iterator_(fileids_iterator),
      status_(Status::OK()) {
    log::trace("RegularIterator::ctor()", "start");
    log::trace("RegularIterator::ctor()", "fileids_iterator_->size():%u", fileids_iterator_->size());
  }

  ~RegularIterator() {
    Close();
  }

  void Close() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!is_closed_) {
      is_closed_ = true;
      if (snapshot_ != nullptr) {
        delete snapshot_;
        snapshot_ = nullptr;
      }
    }
  }

  RegularIterator(RegularIterator&& it)
    : mutex_() {
    log::trace("RegularIterator::move-ctor()", "start");
    this->se_readonly_ = it.se_readonly_;
    this->read_options_ = it.read_options_;
    this->snapshot_ = it.snapshot_;
    this->fileids_iterator_ = it.fileids_iterator_;
    this->is_closed_ = it.is_closed_;
    it.snapshot_ = nullptr;
  }

  void SetParentSnapshot(KingDB *snapshot) {
    snapshot_ = snapshot;
  }

  void Begin() {
    log::trace("RegularIterator::Begin()", "start");
    if (se_readonly_ == nullptr) {
      is_valid_ = false;
      return;
    }
    mutex_.lock();
    fileid_current_ = 0;
    has_file_ = false;
    index_fileid_ = 0;
    is_valid_ = true;
    mutex_.unlock();
    Next();
    log::trace("RegularIterator::Begin()", "end");
  }

  bool IsValid() {
    log::trace("RegularIterator::IsValid()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    log::trace("RegularIterator::IsValid()", "end");
    return is_valid_;
  }

  bool Next() {
    log::trace("RegularIterator::Next()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    if (!is_valid_) return false;
    status_ = Status::OK();
    Status s;

    while (true) {
      log::trace("RegularIterator::Next()", "loop index_file:[%u] index_location:[%u]", index_fileid_, index_location_);
      if (index_fileid_ >= fileids_iterator_->size()) {
        log::trace("RegularIterator::Next()", "invalid index_fileid_:[%u] fileids_iterator_->size():[%u]", index_fileid_, fileids_iterator_->size());
        is_valid_ = false;
        break;
      }

      if (!has_file_) {
        log::trace("RegularIterator::Next()", "initialize file");
        fileid_current_ = fileids_iterator_->at(index_fileid_);
        filepath_current_ = se_readonly_->GetFilepath(fileid_current_);
        struct stat info;
        if (stat(filepath_current_.c_str(), &info) != 0) {
          index_fileid_ += 1;
          continue;
        }
        Mmap mmap(filepath_current_.c_str(), info.st_size);
        if (!mmap.is_valid()) break;
        uint64_t dummy_filesize;
        bool dummy_is_file_large;
        std::multimap<uint64_t, uint64_t> index_temp;
        s = HSTableManager::LoadFile(mmap,
                                     fileid_current_,
                                     index_temp,
                                     &dummy_filesize,
                                     &dummy_is_file_large);
        if (!s.IsOK()) {
          index_fileid_ += 1;
          continue;
        }
        locations_current_.clear();
        for (auto& p: index_temp) {
          locations_current_.push_back(p.second);
        }
        std::sort(locations_current_.begin(), locations_current_.end());
        index_location_ = 0;
        has_file_ = true;
      }

      log::trace("RegularIterator::Next()", "has file");
      if (index_location_ >= locations_current_.size()) {
        log::trace("RegularIterator::Next()", "index_location_ is out");
        has_file_ = false;
        index_fileid_ += 1;
        continue;
      }

      // Get entry at the location
      ByteArray key, value;
      uint64_t location_current = locations_current_[index_location_];
      Status s = se_readonly_->GetEntry(read_options_, location_current, &key, &value);
      if (!s.IsOK()) {
        log::trace("RegularIterator::Next()", "GetEntry() failed: %s", s.ToString().c_str());
        index_location_ += 1;
        continue;
      }

      // Get entry for the key found at the location, and continue if the
      // location is a mismatch -- i.e. the current entry has been overwritten
      // by a later entry.
      
      bool is_last = se_readonly_->IsLocationLastInIndex(location_current, key);
      if (!is_last) {
        //fprintf(stderr, "was not last, need to check\n");
        ByteArray value_alt;
        uint64_t location_out;
        s = se_readonly_->Get(read_options_, key, &value_alt, &location_out);
        if (!s.IsOK()) {
          log::trace("RegularIterator::Next()", "Get(): failed: %s", s.ToString().c_str());
          index_fileid_ += 1;
          continue;
        }
          
        if (location_current != location_out) {
          log::trace("RegularIterator::Next()", "Get(): wrong location - 0x%08" PRIx64 " - 0x%08" PRIx64, location_current, location_out);
          index_location_ += 1;
          continue;
        }
      }

      log::trace("RegularIterator::Next()", "has a valid key/value pair");
      key_ = key;
      value_ = value;
      index_location_ += 1;

      if (value_.size() > se_readonly_->db_options_.internal__size_multipart_required) {
        status_ = Status::MultipartRequired();
      }
      //index_location_ += 1;

      return true;
    }

    return false;
  }

  ByteArray GetKey() {
    std::unique_lock<std::mutex> lock(mutex_);
    return key_;
  }

  ByteArray GetValue() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!value_.is_compressed()) return value_;

    if (value_.size() > se_readonly_->db_options_.internal__size_multipart_required) {
      return ByteArray();
    }

    // TODO-36: Uncompression should not have to go through a MultipartReader. See
    //          the notes about this TODO in kingdb.cc.
    char* buffer = new char[value_.size()];
    uint64_t offset = 0;
    MultipartReader mp_reader(read_options_, value_);
    for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
      ByteArray part;
      mp_reader.GetPart(&part);
      log::trace("ByteArray::GetValue()", "Multipart loop size:%d [%s]", part.size(), part.ToString().c_str());
      memcpy(buffer + offset, part.data(), part.size());
      offset += part.size();
    }
    status_ = mp_reader.GetStatus();
    if (!status_.IsOK()) log::trace("ByteArray::GetValue()", "Error in GetValue(): %s\n", status_.ToString().c_str());
    return ByteArray::NewShallowCopyByteArray(buffer, value_.size());
  }

  MultipartReader GetMultipartValue() {
    return MultipartReader(read_options_, value_);
  }

  Status GetStatus() {
    std::unique_lock<std::mutex> lock(mutex_);
    return status_;
  }

 private:
  StorageEngine *se_readonly_;
  KingDB* snapshot_;
  ReadOptions read_options_;
  std::mutex mutex_;
  uint32_t fileid_current_;
  std::string filepath_current_;
  uint32_t index_fileid_;
  std::vector<uint32_t>* fileids_iterator_;
  uint32_t index_location_;
  std::vector<uint64_t> locations_current_;
  bool has_file_;
  bool is_valid_;
  Status status_;
  bool is_closed_;

  ByteArray key_;
  ByteArray value_;
};



class SequentialIterator: public IteratorResource {
 public:
  SequentialIterator()
    : is_closed_(true),
      se_readonly_(nullptr),
      snapshot_(nullptr),
      status_(Status::IOError("Invalid iterator")) {
  }

  SequentialIterator(ReadOptions& read_options,
           StorageEngine *se_readonly,
           std::vector<uint32_t>* fileids_iterator)
    : is_closed_(false),
      se_readonly_(se_readonly),
      read_options_(read_options),
      snapshot_(nullptr),
      fileids_iterator_(fileids_iterator),
      status_(Status::OK()) {
    log::trace("SequentialIterator::ctor()", "start");
    log::trace("SequentialIterator::ctor()", "fileids_iterator_->size():%u", fileids_iterator_->size());
  }

  ~SequentialIterator() {
    Close();
  }

  void Close() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!is_closed_) {
      is_closed_ = true;
      if (snapshot_ != nullptr) {
        delete snapshot_;
        snapshot_ = nullptr;
      }
    }
  }

  SequentialIterator(SequentialIterator&& it)
    : mutex_() {
    log::trace("SequentialIterator::move-ctor()", "start");
    this->se_readonly_ = it.se_readonly_;
    this->read_options_ = it.read_options_;
    this->snapshot_ = it.snapshot_;
    this->fileids_iterator_ = it.fileids_iterator_;
    this->is_closed_ = it.is_closed_;
    it.snapshot_ = nullptr;
  }

  void SetParentSnapshot(KingDB *snapshot) {
    snapshot_ = snapshot;
  }

  void Begin() {
    log::trace("SequentialIterator::Begin()", "start");
    if (se_readonly_ == nullptr) {
      is_valid_ = false;
      return;
    }
    mutex_.lock();
    fileid_current_ = 0;
    has_file_ = false;
    index_fileid_ = 0;
    is_valid_ = true;
    mutex_.unlock();
    Next();
    log::trace("SequentialIterator::Begin()", "end");
  }

  bool IsValid() {
    log::trace("SequentialIterator::IsValid()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    log::trace("SequentialIterator::IsValid()", "end");
    return is_valid_;
  }

  bool Next() {
    log::trace("SequentialIterator::Next()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    if (!is_valid_) return false;
    status_ = Status::OK();
    Status s;

    while (true) {
      log::trace("SequentialIterator::Next()", "loop index_file:[%u] index_location:[%u]", index_fileid_, index_location_);
      if (index_fileid_ >= fileids_iterator_->size()) {
        log::trace("SequentialIterator::Next()", "invalid index_fileid_:[%u] fileids_iterator_->size():[%u]", index_fileid_, fileids_iterator_->size());
        is_valid_ = false;
        break;
      }

      if (has_file_ && offset_ >= offset_end_) {
        //index_fileid_ += 1;
        //has_file_ = false;
        //continue;
      }

      if (!has_file_) {
        log::trace("SequentialIterator::Next()", "initialize file");
        fileid_current_ = fileids_iterator_->at(index_fileid_);
        filepath_current_ = se_readonly_->GetFilepath(fileid_current_);
        offset_ = se_readonly_->db_options_.internal__hstable_header_size;
        struct stat info;
        if (stat(filepath_current_.c_str(), &info) != 0) {
          index_fileid_ += 1;
          continue;
        }
        mmap_.Open(filepath_current_, info.st_size);
        if (!mmap_.is_valid()) break;
        index_location_ = 0;
        has_file_ = true;

        struct HSTableFooter footer;
        s = HSTableFooter::DecodeFrom(mmap_.datafile() + mmap_.filesize() - HSTableFooter::GetFixedSize(),
                                      HSTableFooter::GetFixedSize(),
                                      &footer);
        offset_end_ = footer.offset_indexes;
      }

      struct EntryHeader entry_header;
      uint32_t size_header;
      Status s = EntryHeader::DecodeFrom(se_readonly_->db_options_, mmap_.datafile() + offset_, mmap_.filesize() - offset_, &entry_header, &size_header);
      if (   !s.IsOK()
          || !entry_header.AreSizesValid(offset_, mmap_.filesize())) {
        // End of file during recovery, thus breaking out of the while-loop
        mmap_.Close();
        has_file_ = false;
        index_fileid_ += 1;
        continue;
      }

      //ByteArray key = ByteArray::NewMmappedByteArray(filepath, mmap_.filesize());
      ByteArray key = ByteArray::NewPooledByteArray(se_readonly_->file_manager_, fileid_current_, filepath_current_, mmap_.filesize_);
      ByteArray value = key;

      if (read_options_.verify_checksums) {
        uint32_t crc32_headerkey = crc32c::Value(value.data() + offset_ + 4, size_header + entry_header.size_key - 4);
        value.set_checksum_initial(crc32_headerkey);
      }


      key.set_offset(offset_ + size_header);
      key.set_size(entry_header.size_key);

      value.set_offset(offset_ + size_header + entry_header.size_key);
      value.set_size(entry_header.size_value);
      value.set_size_compressed(entry_header.size_value_compressed);
      value.set_checksum(entry_header.crc32);

      offset_ += size_header + entry_header.size_key + entry_header.size_value_offset();

      log::trace("SequentialIterator::Next()", "has a valid key/value pair");
      key_ = key;
      value_ = value;
      index_location_ += 1;

      if (value_.size() > se_readonly_->db_options_.internal__size_multipart_required) {
        status_ = Status::MultipartRequired();
      }

      return true;
    }

    return false;
  }

  ByteArray GetKey() {
    std::unique_lock<std::mutex> lock(mutex_);
    return key_;
  }

  ByteArray GetValue() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!value_.is_compressed()) return value_;

    if (value_.size() > se_readonly_->db_options_.internal__size_multipart_required) {
      return ByteArray();
    }

    // TODO-36: Uncompression should not have to go through a MultipartReader. See
    //          the notes about this TODO in kingdb.cc.
    char* buffer = new char[value_.size()];
    uint64_t offset = 0;
    MultipartReader mp_reader(read_options_, value_);
    for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
      ByteArray part;
      mp_reader.GetPart(&part);
      log::trace("ByteArray::GetValue()", "Multipart loop size:%d [%s]", part.size(), part.ToString().c_str());
      memcpy(buffer + offset, part.data(), part.size());
      offset += part.size();
    }
    status_ = mp_reader.GetStatus();
    if (!status_.IsOK()) log::trace("ByteArray::GetValue()", "Error in GetValue(): %s\n", status_.ToString().c_str());
    return ByteArray::NewShallowCopyByteArray(buffer, value_.size());
  }

  MultipartReader GetMultipartValue() {
    return MultipartReader(read_options_, value_);
  }

  Status GetStatus() {
    std::unique_lock<std::mutex> lock(mutex_);
    return status_;
  }

 private:
  StorageEngine *se_readonly_;
  KingDB* snapshot_;
  ReadOptions read_options_;
  std::mutex mutex_;
  uint32_t fileid_current_;
  std::string filepath_current_;
  uint32_t offset_;
  uint32_t offset_end_;
  uint32_t index_fileid_;
  std::vector<uint32_t>* fileids_iterator_;
  uint32_t index_location_;
  std::vector<uint64_t> locations_current_;
  bool has_file_;
  bool is_valid_;
  Status status_;
  bool is_closed_;
  Mmap mmap_;

  ByteArray key_;
  ByteArray value_;
};






class Iterator {
 friend class Snapshot;
 public:
  Iterator(): resource_(nullptr) {}
  ~Iterator() {
    if (resource_ != nullptr) {
      delete resource_; 
      resource_ = nullptr; 
    }
  }

  Iterator(Iterator&& ir) {
    this->resource_ = ir.resource_;
    ir.resource_ = nullptr;
  }

  void Close() {
    resource_->Close();
  }

  void SetParentSnapshot(KingDB *snapshot) {
    resource_->SetParentSnapshot(snapshot);
  }

  void Begin() {
    resource_->Begin();
  }

  bool IsValid() {
    return resource_->IsValid();
  }

  bool Next() {
    return resource_->Next();
  }

  ByteArray GetKey() {
    return resource_->GetKey();
  }

  ByteArray GetValue() {
    return resource_->GetValue();
  }

  MultipartReader GetMultipartValue() {
    return resource_->GetMultipartValue();
  }

  Status GetStatus() {
    return resource_->GetStatus();
  }

  bool _DEBUGGING_IsSequential() {
    // Warning: for unit tests only, do not use this method.
    if (SequentialIterator* d = dynamic_cast<SequentialIterator*>(resource_)) {
      return true;
    } else {
      return false;
    }
  }

 private:
  IteratorResource* resource_;

  void SetIteratorResource(IteratorResource* resource) {
    resource_ = resource; 
  }

};



} // end namespace kdb

#endif // KINGDB_ITERATOR_MAIN_H_
