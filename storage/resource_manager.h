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

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>

#include "kingdb/kdb.h"
#include "kingdb/options.h"
#include "algorithm/hash.h"
#include "kingdb/common.h"
#include "kingdb/byte_array.h"
#include "algorithm/crc32c.h"
#include "util/file.h"


namespace kdb {

class FileResourceManager {
 public:
  FileResourceManager() {
  }

  void ResetDataForFileId(uint32_t fileid) {
    num_writes_in_progress_.erase(fileid);
    logindexes_.erase(fileid);
    has_padding_in_values_.erase(fileid);
  }

  uint64_t GetFileSize(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    return filesizes_[fileid];
  }

  void SetFileSize(uint32_t fileid, uint64_t filesize) {
    std::unique_lock<std::mutex> lock(mutex_);
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
    compactedfiles_.insert(fileid);
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
    return num_writes_in_progress_[fileid];
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

 private:
  std::mutex mutex_;
  std::map<uint32_t, uint64_t> filesizes_;
  std::set<uint32_t> largefiles_;
  std::set<uint32_t> compactedfiles_;
  std::map<uint32_t, uint64_t> num_writes_in_progress_;
  std::map<uint32_t, std::vector< std::pair<uint64_t, uint32_t> > > logindexes_;
  std::set<uint32_t> has_padding_in_values_;
};

} // namespace kdb

#endif // KINGDB_RESOURCE_MANAGER_H_
