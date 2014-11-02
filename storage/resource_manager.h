// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_RESOURCE_MANAGER_H_
#define KINGDB_RESOURCE_MANAGER_H_

#include <thread>
#include <mutex>
#include <chrono>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <cstdio>
#include <ctime>

#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>

#include "kingdb/kdb.h"
#include "util/options.h"
#include "algorithm/hash.h"
#include "util/byte_array.h"
#include "algorithm/crc32c.h"
#include "util/file.h"


namespace kdb {

class FileResourceManager {
 public:
  FileResourceManager() {
    Reset();
  }

  void Reset() {
    std::unique_lock<std::mutex> lock(mutex_);
    dbsize_total_ = 0;
    dbsize_uncompacted_ = 0;
    filesizes_.clear();
    largefiles_.clear();
    compactedfiles_.clear();
    num_writes_in_progress_.clear();
    logindexes_.clear();
    has_padding_in_values_.clear();
    epoch_last_activity_.clear();
  }

  void ClearTemporaryDataForFileId(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    num_writes_in_progress_.erase(fileid);
    logindexes_.erase(fileid);
    has_padding_in_values_.erase(fileid);
    epoch_last_activity_.erase(fileid);
  }

  void ClearAllDataForFileId(uint32_t fileid) {
    ClearTemporaryDataForFileId(fileid);
    std::unique_lock<std::mutex> lock(mutex_);
    uint64_t filesize = 0;
    if (filesizes_.find(fileid) != filesizes_.end()) {
      filesize = filesizes_[fileid];
    }
    IncrementDbSizeTotal(-filesize);
    if (compactedfiles_.find(fileid) == compactedfiles_.end()) {
      IncrementDbSizeUncompacted(-filesize);
    }
    filesizes_.erase(fileid);
    largefiles_.erase(fileid);
    compactedfiles_.erase(fileid);
  }

  uint64_t GetFileSize(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return filesizes_[fileid];
  }

  void SetFileSize(uint32_t fileid, uint64_t filesize) {
    std::unique_lock<std::mutex> lock(mutex_);

    uint64_t filesize_before = 0;
    if (filesizes_.find(fileid) != filesizes_.end()) {
      filesize_before = filesizes_[fileid];
    }
    IncrementDbSizeTotal(filesize - filesize_before);
    if (compactedfiles_.find(fileid) == compactedfiles_.end()) {
      IncrementDbSizeUncompacted(filesize - filesize_before);
    }

    filesizes_[fileid] = filesize;
  }

  bool IsFileLarge(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return (largefiles_.find(fileid) != largefiles_.end());
  }

  void SetFileLarge(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    largefiles_.insert(fileid);
  }

  bool IsFileCompacted(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return (compactedfiles_.find(fileid) != compactedfiles_.end());
  }

  void SetFileCompacted(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (compactedfiles_.find(fileid) != compactedfiles_.end()) return;
    compactedfiles_.insert(fileid);
    if (filesizes_.find(fileid) != filesizes_.end()) {
      // The size for this file was already set, thus the size of uncompacted
      // files needs to be updated.
      IncrementDbSizeUncompacted(-filesizes_[fileid]);
    }
  }

  uint32_t GetNumWritesInProgress(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return num_writes_in_progress_[fileid];
  }

  uint32_t SetNumWritesInProgress(uint32_t fileid, int inc) {
    // The number of writers to a specific file is being tracked so that if a
    // file is flushed but is still being written to due to some multi-chunk
    // entry, we don't write the footer yet. That way, if any crash happens,
    // the file will have no footer, which will force a recovery and discover
    // which entries have corrupted data.
    std::unique_lock<std::mutex> lock(mutex_);
    if (num_writes_in_progress_.find(fileid) == num_writes_in_progress_.end()) {
      num_writes_in_progress_[fileid] = 0;
    }
    num_writes_in_progress_[fileid] += inc;
    epoch_last_activity_[fileid] = std::time(0);
    return num_writes_in_progress_[fileid];
  }

  time_t GetEpochLastActivity(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return epoch_last_activity_[fileid];
  }

  const std::vector< std::pair<uint64_t, uint32_t> > GetLogIndex(uint32_t fileid) {
    return logindexes_[fileid];
  }

  void AddLogIndex(uint32_t fileid, std::pair<uint64_t, uint32_t> p) {
    logindexes_[fileid].push_back(p);
  }

  bool HasPaddingInValues(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return (has_padding_in_values_.find(fileid) != has_padding_in_values_.end());
  }

  void SetHasPaddingInValues(uint32_t fileid, bool flag) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (flag) {
      has_padding_in_values_.insert(fileid);
    } else {
      has_padding_in_values_.erase(fileid);
    }
  }

  uint64_t GetDbSizeTotal() {
    std::unique_lock<std::mutex> lock(mutex_dbsize_);
    return dbsize_total_;
  }

  uint64_t GetDbSizeUncompacted() {
    std::unique_lock<std::mutex> lock(mutex_dbsize_);
    return dbsize_uncompacted_;
  }

  void IncrementDbSizeTotal(int64_t inc) {
    std::unique_lock<std::mutex> lock(mutex_dbsize_);
    assert(dbsize_total_ + inc >= 0);
    dbsize_total_ += inc;
  }

  void IncrementDbSizeUncompacted(int64_t inc) {
    std::unique_lock<std::mutex> lock(mutex_dbsize_);
    assert(dbsize_uncompacted_ + inc >= 0);
    dbsize_uncompacted_ += inc;
  }

 private:
  // NOTE: all files go through the same mutexes -- this can easily be sharded
  std::mutex mutex_;
  std::mutex mutex_dbsize_;
  std::map<uint32_t, uint64_t> filesizes_;
  std::set<uint32_t> largefiles_;
  std::set<uint32_t> compactedfiles_;
  std::map<uint32_t, uint64_t> num_writes_in_progress_;
  std::map<uint32_t, std::vector< std::pair<uint64_t, uint32_t> > > logindexes_;
  std::set<uint32_t> has_padding_in_values_;
  std::map<uint32_t, time_t> epoch_last_activity_;
  uint64_t dbsize_total_;
  uint64_t dbsize_uncompacted_;
};

} // namespace kdb

#endif // KINGDB_RESOURCE_MANAGER_H_
