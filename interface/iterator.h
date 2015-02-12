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
#include "interface/interface.h"
#include "storage/storage_engine.h"

namespace kdb {

class BasicIterator: public Iterator {
 public:
  BasicIterator(ReadOptions& read_options,
                   StorageEngine *se_readonly,
                   std::vector<uint32_t>* fileids_iterator)
      : se_readonly_(se_readonly),
        read_options_(read_options),
        snapshot_(nullptr),
        fileids_iterator_(fileids_iterator) {
    log::trace("BasicIterator::ctor()", "start");
  }

  ~BasicIterator() {
    log::emerg("BasicIterator::dtor()", "call");
    if (key_ != nullptr) {
      delete key_;
      delete value_;
      key_ = nullptr;
      value_ = nullptr;
    }
    if (snapshot_ != nullptr) {
      delete snapshot_;
    }
  }

  void SetParentSnapshot(Interface *snapshot) {
    snapshot_ = snapshot; 
  }

  void Begin() {
    log::trace("BasicIterator::Begin()", "start");
    mutex_.lock();
    fileid_current_ = 0;
    has_file_ = false;
    index_fileid_ = 0;
    is_valid_ = true;
    key_ = nullptr;
    value_ = nullptr;
    mutex_.unlock();
    Next();
    log::trace("BasicIterator::Begin()", "end");
  }

  bool IsValid() {
    log::trace("BasicIterator::IsValid()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    log::trace("BasicIterator::IsValid()", "end");
    return is_valid_;
  }

  bool Next() {
    log::trace("BasicIterator::Next()", "start");
    std::unique_lock<std::mutex> lock(mutex_);
    if (!is_valid_) return false;
    Status s;

    while (true) { 
      if (key_ != nullptr) {
        delete key_;
        delete value_;
        key_ = nullptr;
        value_ = nullptr;
      }
      log::trace("BasicIterator::Next()", "loop index_file:[%u] index_location:[%u]", index_fileid_, index_location_);
      if (index_fileid_ >= fileids_iterator_->size()) {
        is_valid_ = false;
        break;
      }

      if (!has_file_) {
        log::trace("BasicIterator::Next()", "initialize file");
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
        key_ = nullptr;
        value_ = nullptr;
        has_file_ = true;
      }

      log::trace("BasicIterator::Next()", "has file");
      if (index_location_ >= locations_current_.size()) {
        log::trace("BasicIterator::Next()", "index_location_ is out");
        has_file_ = false;
        index_fileid_ += 1;
        continue;
      }

      // Get entry at the location
      ByteArray *key = nullptr;
      ByteArray *value = nullptr;
      uint64_t location_current = locations_current_[index_location_];
      Status s = se_readonly_->GetEntry(read_options_, location_current, &key, &value);
      if (!s.IsOK()) {
        log::trace("BasicIterator::Next()", "GetEntry() failed: %s", s.ToString().c_str());
        delete key; 
        delete value;
        index_location_ += 1;
        continue;
      }

      // Get entry for the key found at the location, and continue if the
      // locations mismatch -- i.e. the current entry has been overwritten
      // by a later entry.
      ByteArray *value_alt = nullptr;
      uint64_t location_out;
      s = se_readonly_->Get(read_options_, key, &value_alt, &location_out);
      if (!s.IsOK()) {
        log::trace("BasicIterator::Next()", "Get(): failed: %s", s.ToString().c_str());
        delete key;
        delete value;
        delete value_alt;
        index_fileid_ += 1;
        continue;
      }
        
      if (location_current != location_out) {
        log::trace("BasicIterator::Next()", "Get(): wrong location");
        delete key;
        delete value;
        delete value_alt;
        index_location_ += 1;
        continue;
      }

      log::trace("BasicIterator::Next()", "has a valid key/value pair");
      key_ = key;
      value_ = value;
      delete value_alt;
      index_location_ += 1;
      return true;
    }

    return false;
  }

  ByteArray *GetKey() {
    return key_;
  }

  ByteArray *GetValue() {
    return value_;
  }

 private:
  StorageEngine *se_readonly_;
  Interface *snapshot_;
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

  ByteArray* key_;
  ByteArray* value_;
};

} // end namespace kdb

#endif // KINGDB_ITERATOR_MAIN_H_
