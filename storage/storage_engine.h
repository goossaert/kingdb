// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_STORAGE_ENGINE_H_
#define KINGDB_STORAGE_ENGINE_H_

#include "util/debug.h"
#include <thread>
#include <mutex>
#include <chrono>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <cstdio>
#include <inttypes.h>

#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>

#include "kingdb/kdb.h"
#include "util/options.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/file.h"
#include "algorithm/crc32c.h"
#include "algorithm/hash.h"
#include "storage/format.h"
#include "storage/resource_manager.h"
#include "storage/hstable_manager.h"


namespace kdb {

class StorageEngine {
 friend class Iterator;
 public:
  StorageEngine(DatabaseOptions db_options,
                EventManager *event_manager,
                std::string dbname,
                bool read_only=false, // TODO: this should be part of db_options -- sure about that? what options are stored on disk?
                std::set<uint32_t>* fileids_ignore=nullptr,
                uint32_t fileid_end=0)
      : db_options_(db_options),
        event_manager_(event_manager),
        is_read_only_(read_only),
        prefix_compaction_("compaction_"),
        dirpath_locks_(dbname + "/locks"),
        hstable_manager_(db_options, dbname, "", prefix_compaction_, dirpath_locks_, kUncompactedRegularType, read_only),
        hstable_manager_compaction_(db_options, dbname, prefix_compaction_, prefix_compaction_, dirpath_locks_, kCompactedRegularType, read_only) {
    log::trace("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    fileids_ignore_ = fileids_ignore;
    num_readers_ = 0;
    is_compaction_in_progress_ = false;
    sequence_snapshot_ = 0;
    stop_requested_ = false;
    is_closed_ = false;
    fs_free_space_ = 0;
    if (!is_read_only_) {
      thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
      thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
      thread_compaction_ = std::thread(&StorageEngine::ProcessingLoopCompaction, this);
      thread_statistics_ = std::thread(&StorageEngine::ProcessingLoopStatistics, this);
    }
    hash_ = MakeHash(db_options.hash);
    if (!is_read_only_) {
      fileids_iterator_ = nullptr;
    } else {
      fileids_iterator_ = new std::vector<uint32_t>();
    }
    Status s = hstable_manager_.LoadDatabase(dbname, index_, fileids_ignore_, fileid_end, fileids_iterator_);
    if (!s.IsOK()) {
      log::emerg("StorageEngine", "Could not load database: [%s]", s.ToString().c_str());
      Close();
    }
  }

  ~StorageEngine() {}

  static std::string GetCompactionFilePrefix() {
    return "compaction_"; 
  }

  void Close() {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    is_closed_ = true;

    // Wait for readers to exit
    AcquireWriteLock();
    hstable_manager_.Close();
    Stop();
    ReleaseWriteLock();

    if (!is_read_only_) {
      log::trace("StorageEngine::Close()", "join start");
      event_manager_->update_index.NotifyWait(); // notifies ProcessingLoopIndex()
      event_manager_->flush_buffer.NotifyWait(); // notifies ProcessingLoopData()
      cv_statistics_.notify_all();               // notifies ProcessingLoopStatistics()
      cv_loop_compaction_.notify_all();          // notifies ProcessingLoopCompaction()
      thread_index_.join();
      thread_data_.join();
      thread_compaction_.join();
      thread_statistics_.join();
      Status s = ReleaseAllSnapshots();
      if (!s.IsOK()) {
        log::emerg("StorageEngine::Close()", s.ToString().c_str());
      }
      log::trace("StorageEngine::Close()", "join end");
    }

    if (fileids_ignore_ != nullptr) {
      delete fileids_ignore_; 
    }

    if (fileids_iterator_ != nullptr) {
      delete fileids_iterator_; 
    }

    delete hash_;

    log::trace("StorageEngine::Close()", "done");
  }

  bool IsStopRequested() { return stop_requested_; }
  void Stop() {
    stop_requested_ = true;
  }

  void ProcessingLoopStatistics() {
    std::chrono::milliseconds duration(db_options_.compaction__check_interval);
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_statistics_);
      fs_free_space_ = FileUtil::fs_free_space(dbname_.c_str());
      cv_statistics_.wait_for(lock, duration);
      if (IsStopRequested()) return;
    }
  }

  uint64_t GetFreeSpace() {
    std::unique_lock<std::mutex> lock(mutex_statistics_);
    return fs_free_space_;
  }

  Status FileSystemStatus() {
    if (GetFreeSpace() < db_options_.storage__minimum_free_space_accept_orders) {
      return Status::IOError("Not enough free space on the file system");
    } else if (!hstable_manager_.CanOpenNewFiles()) {
      return Status::IOError("Cannot open new files");
    }
    return Status::OK();
  }

  void ProcessingLoopCompaction() {
    // 1. Have a ProcessingLoopStatistics() which pull the disk usage and
    //    dbsize values every 'db.compaction.check_interval' milliseconds.
    // 2. 'fileid_lastcompacted' hold the id of the last hstable that was
    //    successfully compacted.
    // 3. If the free disk space is > db.compaction.filesystem.survival_mode_threshold
    //      mode = normal
    //      batch_size = db.compaction.filesystem.normal_batch_size
    //    else
    //      mode = survival
    //      batch_size = db.compaction.filesystem.survival_batch_size
    //
    //    If free disk space > db.compaction.filesystem.free_space_required
    //       &&
    //       sum all uncompacted files > batch_size
    //      Do compaction by going to step 4
    //    else:
    //      Sleep for a duration of db.compaction.check_interval
    //      Go to step 3
    // 4. Initialize: M = M_default
    // 5. In compaction:
    //    - Scan through uncompacted files until the sum of their sizes reaches M
    //    - Try reserving the disk space necessary for the compaction process
    //    - If compaction fails, do M <- M/2 and goto step 5
    //    - If M reaches 0, try one compaction run (trying to clear the large
    //      files if any), and if still unsuccessful, declare compaction impossible
    // 6. If the compaction succeeded, update 'fileid_lastcompacted'
 
    std::chrono::milliseconds duration(db_options_.storage__statistics_polling_interval);
    uint32_t fileid_lastcompacted = 0;
    uint32_t fileid_out = 0;

    while (true) {
      uint64_t size_compaction = 0;
      uint64_t fs_free_space = GetFreeSpace();
      if (fs_free_space > db_options_.compaction__filesystem__survival_mode_threshold) {
        size_compaction = db_options_.compaction__filesystem__normal_batch_size;
      } else {
        size_compaction = db_options_.compaction__filesystem__survival_batch_size;
      }
 
      // Only files that are no longer taking incoming updates can be compacted
      uint32_t fileid_end = hstable_manager_.GetHighestStableFileId(fileid_lastcompacted + 1);
      
      uint64_t dbsize_uncompacted = hstable_manager_.file_resource_manager.GetDbSizeUncompacted();
      log::trace("ProcessingLoopCompaction",
                "fileid_end:%u fs_free_space:%" PRIu64 " compaction.filesystem.free_space_required:%" PRIu64 " size_compaction:%" PRIu64 " dbsize_uncompacted:%" PRIu64,
                fileid_end,
                fs_free_space,
                db_options_.compaction__filesystem__free_space_required,
                size_compaction,
                dbsize_uncompacted);

      if (   fileid_end > 0
          && fs_free_space > db_options_.compaction__filesystem__free_space_required
          && dbsize_uncompacted > size_compaction) {
        while (true) {
          fileid_out = 0;
          Status s = Compaction(dbname_, fileid_lastcompacted + 1, fileid_end, size_compaction, &fileid_out);
          if (!s.IsOK()) {
            if (size_compaction == 0) break;
            size_compaction /= 2;
          } else {
            fileid_lastcompacted = fileid_out;  
            break;
          }
          if (IsStopRequested()) return;
        }
      }

      std::unique_lock<std::mutex> lock(mutex_loop_compaction_);
      cv_loop_compaction_.wait_for(lock, duration);
      if (IsStopRequested()) return;
    }
  }

  void ProcessingLoopData() {
    // NOTE: Failures are handled as such:
    // - If the entry is small, there is no failure handling: the entry either
    //   is already full, or it didn't make it that far.
    // - If the entry is a multipart entry, it is possible that a part doesn't
    //   make it to the Storage Engine. In that case, the Offset Array for the
    //   HSTable of the entry will not be written, and the entry will not be
    //   saved in the index either.
    //     * The compaction thread goes over all recent HSTables regularly, and
    //       cleans up the temporary data related to HSTables stored in the
    //       memory, avoiding memory leaks.
    //     * If a compaction processes goes over the HSTable with the invalid
    //       entry, it will simply ignore it and will reclaim the storage space
    //       for the HSTable.
    //     * If the database crashes, the recovery process will fix the file and
    //       the entry.
    // - If the entry is a large entry: the error is not handled and the large
    //   files simply needs to be deleted (see TODO-39).
    while(true) {
      // Wait for orders to process
      log::trace("StorageEngine::ProcessingLoopData()", "start");
      std::vector<Order> orders = event_manager_->flush_buffer.Wait();
      if (IsStopRequested()) return;
      log::trace("StorageEngine::ProcessingLoopData()", "got %d orders", orders.size());

      // Process orders, and create update map for the index
      AcquireWriteLock();
      std::multimap<uint64_t, uint64_t> map_index;
      hstable_manager_.WriteOrdersAndFlushFile(orders, map_index);
      ReleaseWriteLock();

      event_manager_->flush_buffer.Done();
      event_manager_->update_index.StartAndBlockUntilDone(map_index);
    }
  }

  void ProcessingLoopIndex() {
    while(true) {
      log::trace("StorageEngine::ProcessingLoopIndex()", "start");
      std::multimap<uint64_t, uint64_t> index_updates = event_manager_->update_index.Wait();
      if (IsStopRequested()) return;
      log::trace("StorageEngine::ProcessingLoopIndex()", "got index_updates: %d updates", index_updates.size());

      /*
      for (auto& p: index_updates) {
        if (p.second == 0) {
          log::trace("StorageEngine::ProcessingLoopIndex()", "remove [%s] num_items_index [%d]", p.first.c_str(), index_.size());
          index_.erase(p.first);
        } else {
          log::trace("StorageEngine::ProcessingLoopIndex()", "put [%s]", p.first.c_str());
          index_[p.first] = p.second;
        }
      }
      */

      std::multimap<uint64_t, uint64_t> *index;
      mutex_compaction_.lock();
      if (is_compaction_in_progress_) {
        index = &index_compaction_;
      } else {
        index = &index_;
      }
      mutex_compaction_.unlock();


      int num_iterations_per_lock = db_options_.internal__num_iterations_per_lock;
      int counter_iterations = 0;

      for (auto& p: index_updates) {
        if (counter_iterations == 0) {
          AcquireWriteLock();
        }
        counter_iterations += 1;

        //uint64_t hashed_key = hash_->HashFunction(p.first.c_str(), p.first.size());
        //log::trace("StorageEngine::ProcessingLoopIndex()", "hash [%" PRIu64 "] location [%" PRIu64 "]", p.first, p.second);
        //mutex_index_.lock();
        index->insert(std::pair<uint64_t,uint64_t>(p.first, p.second));
        //mutex_index_.unlock();

        // Throttling the index updates, and allows other processes
        // to acquire the write lock if they need it
        if (counter_iterations >= num_iterations_per_lock) {
          ReleaseWriteLock();
          counter_iterations = 0;
        }
      }
      if (counter_iterations) ReleaseWriteLock();

      /*
      for (auto& p: index_) {
        log::trace("index_", "hash:[0x%08x] location:[%" PRIu64 "]", p.first, p.second);
      }
      */

      event_manager_->update_index.Done();
      log::trace("StorageEngine::ProcessingLoopIndex()", "done");
      int temp = 1;
      event_manager_->clear_buffer.StartAndBlockUntilDone(temp);
    }
  }

  Status Get(ReadOptions& read_options,
             ByteArray& key,
             ByteArray* value_out,
             uint64_t *location_out=nullptr) {
    // NOTE: There is no monitoring or reference counting for the file used by
    //       a ByteArray. If the user has a ByteArray pointing to a file, and
    //       this file is deleted by the compaction process, this could lead
    //       to serious issues.
    //       Luckily, in Linux and BSD, the kernel counts open file descriptors
    //       for each file, and files are really deleted only when all the
    //       descriptors are closed. Therefore, it is fine to delete files that
    //       are open: their content will remain available to the file
    //       descriptor holder, and the storage space will be reclaimed when the
    //       file descriptor is closed.
    mutex_write_.lock();
    mutex_read_.lock();
    num_readers_ += 1;
    mutex_read_.unlock();
    mutex_write_.unlock();

    bool has_compaction_index = false;
    mutex_compaction_.lock();
    has_compaction_index = is_compaction_in_progress_;
    mutex_compaction_.unlock();

    Status s;
    if (!has_compaction_index) {
      s = GetWithIndex(read_options, index_, key, value_out, location_out);
    } else {
      s = GetWithIndex(read_options, index_compaction_, key, value_out, location_out);
      if (!s.IsOK() && !s.IsDeleteOrder()) {
        s = GetWithIndex(read_options, index_, key, value_out, location_out);
      }
    }

    mutex_read_.lock();
    num_readers_ -= 1;
    log::trace("Get()", "num_readers_: %d", num_readers_);
    mutex_read_.unlock();
    cv_read_.notify_one();

    return s;
  }


  Status GetWithIndex(ReadOptions& read_options,
                      std::multimap<uint64_t, uint64_t>& index,
                      ByteArray& key,
                      ByteArray* value_out,
                      uint64_t *location_out=nullptr) {
    //std::unique_lock<std::mutex> lock(mutex_index_);
    // TODO-26: should not be locking here, instead, should store the hashed key
    // and location from the index and release the lock right away -- should not
    // be locking while calling GetEntry()

    // NOTE: Since C++11, the relative ordering of elements with equivalent keys
    //       in a multimap is preserved.
    uint64_t hashed_key = hash_->HashFunction(key.data(), key.size());
    log::trace("StorageEngine::GetWithIndex()", "num entries in index:[%d] content:[%s] size:[%d] hashed_key:[0x%" PRIx64 "]", index.size(), key.ToString().c_str(), key.size(), hashed_key);

    auto range = index.equal_range(hashed_key);
    auto rbegin = --range.second;
    auto rend  = --range.first;
    for (auto it = rbegin; it != rend; --it) {
      ByteArray key_temp;
      Status s = GetEntry(read_options, it->second, &key_temp, value_out);
      log::trace("StorageEngine::GetWithIndex()", "key:[%s] key_temp:[%s] hashed_key:[0x%" PRIx64 "] hashed_key_temp:[0x%" PRIx64 "] size_key:[%" PRIu64 "] size_key_temp:[%" PRIu64 "]", key.ToString().c_str(), key_temp.ToString().c_str(), hashed_key, it->first, key.size(), key_temp.size());
      if ((s.IsOK() || s.IsDeleteOrder()) && key_temp == key) {
        log::trace("StorageEngine::GetWithIndex()", "Entry [%s] found at location: 0x%08" PRIx64, key.ToString().c_str(), it->second);
        if (location_out != nullptr) *location_out = it->second;
        return s;
      }
    }
    log::trace("StorageEngine::GetWithIndex()", "%s - not found!", key.ToString().c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }


  Status GetEntry(ReadOptions& read_options,
                  uint64_t location,
                  ByteArray* key_out,
                  ByteArray* value_out) {
    log::trace("StorageEngine::GetEntry()", "start");
    Status s = Status::OK();
    // TODO: check that the offset falls into the
    // size of the file, just in case a file was truncated but the index
    // still had a pointer to an entry in at an invalid location --
    // alternatively, we could just let the host program crash, to force a restart
    // which would rebuild the index properly

    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    uint64_t filesize = 0;
    filesize = hstable_manager_.file_resource_manager.GetFileSize(fileid);

    log::trace("StorageEngine::GetEntry()", "location:%" PRIu64 " fileid:%u offset_file:%u filesize:%" PRIu64, location, fileid, offset_file, filesize);
    std::string filepath = hstable_manager_.GetFilepath(fileid); // TODO: optimize here

    ByteArray key_temp = ByteArray::NewMmappedByteArray(filepath, filesize);
    ByteArray value_temp = key_temp;
    // NOTE: verify that value_temp.size() is indeed filesize -- verified and
    // the size was 0: should the size of an mmapped byte array be the size of
    // the file by default?

    struct EntryHeader entry_header;
    uint32_t size_header;
    s = EntryHeader::DecodeFrom(db_options_, value_temp.data() + offset_file, filesize - offset_file, &entry_header, &size_header);
    if (!s.IsOK()) return s;

    if (   !entry_header.AreSizesValid(offset_file, filesize)
        || !entry_header.IsEntryFull()) {
      entry_header.print();
      return Status::IOError("Entry has invalid header");
    }

    if (read_options.verify_checksums) {
      uint32_t crc32_headerkey = crc32c::Value(value_temp.data() + offset_file + 4, size_header + entry_header.size_key - 4);
      value_temp.set_checksum_initial(crc32_headerkey);
    }

    key_temp.set_offset(offset_file + size_header);
    key_temp.set_size(entry_header.size_key);

    value_temp.set_offset(offset_file + size_header + entry_header.size_key);
    value_temp.set_size(entry_header.size_value);
    value_temp.set_size_compressed(entry_header.size_value_compressed);
    value_temp.set_checksum(entry_header.crc32);
    //PrintHex(value_temp.data(), 16);

    if (entry_header.IsTypeDelete()) {
      s = Status::DeleteOrder();
    }

    log::debug("StorageEngine::GetEntry()", "mmap() out - type remove:%d", entry_header.IsTypeDelete());
    log::trace("StorageEngine::GetEntry()", "Sizes: key_temp:%" PRIu64 " value_temp:%" PRIu64 " size_value_compressed:%" PRIu64 " filesize:%" PRIu64, key_temp.size(), value_temp.size(), value_temp.size_compressed(), filesize);

    *key_out = key_temp;
    *value_out = value_temp;
    return s;
  }

  bool IsFileLarge(uint32_t fileid) {
    return hstable_manager_.file_resource_manager.IsFileLarge(fileid);
  }

  Status Compaction(std::string dbname,
                    uint32_t fileid_start,
                    uint32_t fileid_end_target,
                    uint64_t size_compaction,
                    uint32_t *fileid_out) {
    // NOTE: Depending on what has to be compacted, Compaction() can take a
    //       long time. Therefore IsStopRequested() is called at the end
    //       of each major step to allow the method to exit in case a stop
    //       was requested.

    // TODO: make sure that all sets, maps and multimaps are cleared whenever
    // they are no longer needed
    
    // TODO: when compaction starts, open() a file and lseek() to reserve disk
    //       space -- or write a bunch of files with the "compaction_" prefix
    //       that will be overwritten when the compacted files are written.

    // TODO: add a new flag in files that says "compacted" or "regular", and before
    //       starting any compaction process, select only regular files, ignore
    //       compacted ones. (large files are 'compacted' by default).

    // TODO-23: replace the change on is_compaction_in_progress_ by a RAII
    //          WARNING: this is not the only part of the code with this issue,
    //          some code digging in all files is required
    mutex_compaction_.lock();
    is_compaction_in_progress_ = true;
    mutex_compaction_.unlock();

    // Before the compaction starts, make sure all compaction-related files are removed
    Status s;
    s = FileUtil::remove_files_with_prefix(dbname.c_str(), prefix_compaction_);
    if (!s.IsOK()) return Status::IOError("Could not clean up previous compaction", dbname.c_str());

    // 1a. Get *all* the files that are candidates for compaction
    // TODO: This is a quick hack to get the files for compaction, by going
    //       through all the files. Fix that to be only the latest non-handled
    //       uncompacted files
    log::trace("Compaction()", "Step 1: Get files between fileids %u and %u", fileid_start, fileid_end_target);
    std::multimap<uint64_t, uint64_t> index_compaction;
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }
    if (IsStopRequested()) return Status::IOError("Stop was requested");

    std::map<uint32_t, uint64_t> fileids_to_filesizes;
    char filepath[FileUtil::maximum_path_size()];
    uint32_t fileid = 0;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      if (strcmp(entry->d_name, DatabaseOptions::GetFilename().c_str()) == 0) continue;
      int ret = snprintf(filepath, FileUtil::maximum_path_size(), "%s/%s", dbname.c_str(), entry->d_name);
      if (ret < 0 || ret >= FileUtil::maximum_path_size()) {
        log::emerg("Compaction()",
                  "Filepath buffer is too small, could not build the filepath string for file [%s]", entry->d_name); 
        continue;
      }
      fileid = HSTableManager::hex_to_num(entry->d_name);
      if (   hstable_manager_.file_resource_manager.IsFileCompacted(fileid)
          || stat(filepath, &info) != 0
          || !(info.st_mode & S_IFREG) 
          || fileid < fileid_start
          || fileid > fileid_end_target
          || info.st_size <= db_options_.internal__hstable_header_size) {
        continue;
      }
      fileids_to_filesizes[fileid] = info.st_size;
    }
    closedir(directory);


    // 1b. Filter to process files only up to a certain total size
    //     (large files are ignored)
    uint32_t fileid_end_actual = 0;
    uint64_t size_total = 0;
    for (auto& p: fileids_to_filesizes) {
      // NOTE: Here the locations are read directly from the secondary storage,
      //       which could be optimized by reading them from the index in memory. 
      //       One way to do that is to have a temporary index to which all
      //       updates are synced during compaction. That way, the main index is
      //       guaranteed to not be changed, thus all sorts of scans and changes
      //       can be done on it. Once compaction is over, the temporary index
      //       can just be poured into the main index.
      uint32_t fileid = p.first;
      uint64_t filesize = p.second;
      if (!IsFileLarge(fileid) && size_total + filesize > size_compaction) break;
      fileid_end_actual = fileid;
      *fileid_out = fileid;
      std::string filepath = hstable_manager_.GetFilepath(fileid);
      Mmap mmap(filepath, filesize);
      if (!mmap.is_valid()) return Status::IOError("Mmap constructor failed");
      // TODO-40: Make sure that if the HSTable was invalid, it still has an
      //          Offset Array so that LoadFile() can properly load the entries from it.
      //          The compaction thread handling the inactivity timeout should be taking
      //          care of that.
      s = hstable_manager_.LoadFile(mmap, fileid, index_compaction);
      if (!s.IsOK()) {
        log::warn("HSTableManager::Compaction()", "Could not load index in file [%s]", filepath.c_str());
        // TODO: handle the case where a file is found to be damaged during compaction
      }
      size_total += filesize;
    }
    fileids_to_filesizes.clear(); // no longer needed
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 2. Iterating over all unique hashed keys of index_compaction, and determine which
    // locations of the storage engine index 'index_' with similar hashes will need to be compacted.
    log::trace("Compaction()", "Step 2: Get unique hashed keys");
    std::vector<std::pair<uint64_t, uint64_t>> index_compaction_se;
    for (auto it = index_compaction.begin(); it != index_compaction.end(); it = index_compaction.upper_bound(it->first)) {
      auto range = index_.equal_range(it->first);
      for (auto it_se = range.first; it_se != range.second; ++it_se) {
        index_compaction_se.push_back(*it_se);
      }
    }
    index_compaction.clear(); // no longer needed
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 3. For each entry, determine which location has to be kept, which has to be deleted,
    // and the overall set of file ids that needs to be compacted
    log::trace("Compaction()", "Step 3: Determine locations");
    std::set<uint64_t> locations_delete;
    std::set<uint32_t> fileids_compaction;
    std::set<uint32_t> fileids_largefiles_keep;
    std::set<std::string> keys_encountered;
    std::multimap<uint64_t, uint64_t> hashedkeys_to_locations_regular_keep;
    std::multimap<uint64_t, uint64_t> hashedkeys_to_locations_large_keep;
    // Reversing the order of the vector to guarantee that
    // the most recent locations are treated first
    std::reverse(index_compaction_se.begin(), index_compaction_se.end());
    ReadOptions read_options;
    for (auto &p: index_compaction_se) {
      ByteArray key, value;
      uint64_t& location = p.second;
      uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
      if (fileid > fileid_end_actual) {
        // Make sure that files added after the compacted
        // files or during the compaction itself are not used
        continue;
      }
      fileids_compaction.insert(fileid);
      Status s = GetEntry(read_options, location, &key, &value);
      std::string str_key = key.ToString();

      // For any given key, only the first occurrence, which is the most recent one,
      // has to be kept. The other ones will be deleted. If the first occurrence
      // is a Delete Order, then all occurrences of that key will be deleted.
      if (keys_encountered.find(str_key) == keys_encountered.end()) {
        keys_encountered.insert(str_key);
        if (IsFileLarge(fileid)) {
          hashedkeys_to_locations_large_keep.insert(p);
          fileids_largefiles_keep.insert(fileid);
        } else if (!s.IsDeleteOrder()) {
          hashedkeys_to_locations_regular_keep.insert(p);
        } else {
          locations_delete.insert(location);
        }
      } else {
        locations_delete.insert(location);
      }
    }
    index_compaction_se.clear(); // no longer needed
    keys_encountered.clear(); // no longer needed
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 4. Building the clusters of locations, indexed by the smallest location
    // per cluster. All the non-smallest locations are stored as secondary
    // locations. Only regular entries are used: it would not make sense
    // to compact large entries anyway.
    log::trace("Compaction()", "Step 4: Building clusters");
    std::map<uint64_t, std::vector<uint64_t>> hashedkeys_clusters;
    std::set<uint64_t> locations_secondary;
    for (auto it = hashedkeys_to_locations_regular_keep.begin(); it != hashedkeys_to_locations_regular_keep.end(); it = hashedkeys_to_locations_regular_keep.upper_bound(it->first)) {
      auto range = hashedkeys_to_locations_regular_keep.equal_range(it->first);
      std::vector<uint64_t> locations;
      for (auto it_bucket = range.first; it_bucket != range.second; ++it_bucket) {
        log::trace("Compaction()", "Building clusters - location:%" PRIu64, it->second);
        locations.push_back(it->second);
      }
      std::sort(locations.begin(), locations.end());
      hashedkeys_clusters[locations[0]] = locations;
      for (auto i = 1; i < locations.size(); i++) {
        locations_secondary.insert(locations[i]);
      }
    }
    hashedkeys_to_locations_regular_keep.clear();
    if (IsStopRequested()) return Status::IOError("Stop was requested");

    /*
     * The compaction needs the following collections:
     *
     * - fileids_compaction: fileids of all files on which compaction must operate
     *     set<uint32_t>
     *
     * - fileids_largefiles_keep: set of fileids that contain large items that must be kept
     *     set<uint32_t>
     *
     * - hashedkeys_clusters: clusters of locations having same hashed keys,
     *   sorted by ascending order of hashed keys and indexed by the smallest
     *   location.
     *     map<uint64_t, std::vector<uint64_t>>
     *
     * - locations_secondary: locations of all entries to keep
     *     set<uint64_t>
     *
     * - locations_delete: locations of all entries to delete
     *     set<uint64_t>
     *
     */

    // 5a. Reserving space in the file system
    // Reserve as much space are the files to compact are using, this is a
    // poor approximation, but should cover most cases. Large files are ignored.
    uint32_t fileid_compaction = 1;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (IsFileLarge(fileid)) continue;
      uint64_t filesize = hstable_manager_.file_resource_manager.GetFileSize(fileid);
      std::string filepath = hstable_manager_compaction_.GetFilepath(fileid_compaction);
      Status s = FileUtil::fallocate_filepath(filepath, filesize);
      if (!s.IsOK()) {
        // TODO: the cleanup of the compaction (removals, etc.) should be
        //       mutualized in the processing loop
        FileUtil::remove_files_with_prefix(dbname.c_str(), prefix_compaction_);
        return s;
      }
      fileid_compaction += 1;
    }
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 5b. Mmapping all the files involved in the compaction
    log::trace("Compaction()", "Step 5: Mmap() all the files! ALL THE FILES!");
    std::map<uint32_t, Mmap*> mmaps;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
      struct stat info;
      std::string filepath = hstable_manager_.GetFilepath(fileid);
      if (stat(filepath.c_str(), &info) != 0 || !(info.st_mode & S_IFREG)) {
        log::emerg("Compaction()", "Error during compaction with file [%s]", filepath.c_str());
      }
      Mmap *mmap = new Mmap(filepath.c_str(), info.st_size);
      if (!mmap->is_valid()) {
        delete mmap;
        continue;
      }
      mmaps[fileid] = mmap;
    }
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 6. Now building a vector of orders, that will be passed to the
    //    hstable_manager_compaction_ object to persist them on disk
    log::trace("Compaction()", "Step 6: Build order list");
    std::vector<Order> orders;
    uint64_t timestamp_max = 0;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (IsFileLarge(fileid)) continue;
      Mmap* mmap = mmaps[fileid];

      // Read the header to update the maximimum timestamp
      struct HSTableHeader hstheader;
      s = HSTableHeader::DecodeFrom(mmap->datafile(), mmap->filesize(), &hstheader);
      if (!s.IsOK()) return Status::IOError("Could not read file header during compaction"); // TODO: skip file instead of returning an error 
      timestamp_max = std::max(timestamp_max, hstheader.timestamp);

      // Read the footer to get the offset where entries stop
      struct HSTableFooter footer;
      Status s = HSTableFooter::DecodeFrom(mmap->datafile() + mmap->filesize() - HSTableFooter::GetFixedSize(), HSTableFooter::GetFixedSize(), &footer);
      uint32_t crc32_computed = crc32c::Value(mmap->datafile() + footer.offset_indexes, mmap->filesize() - footer.offset_indexes - 4);
      uint64_t offset_end;
      if (   !s.IsOK()
          || footer.magic_number != HSTableManager::get_magic_number()
          || footer.crc32 != crc32_computed) {
        // TODO: handle error
        offset_end = mmap->filesize();
        log::trace("Compaction()", "Compaction - invalid footer");
      } else {
        offset_end = footer.offset_indexes;
      }

      // Process entries in the file
      uint32_t offset = db_options_.internal__hstable_header_size;
      while (offset < offset_end) {
        log::trace("Compaction()", "order list loop - offset:%u offset_end:%u", offset, offset_end);
        struct EntryHeader entry_header;
        uint32_t size_header;
        Status s = EntryHeader::DecodeFrom(db_options_, mmap->datafile() + offset, mmap->filesize() - offset, &entry_header, &size_header);

        // NOTE: No need to verify the checksum. See notes in RecoverFile().
        if (   !s.IsOK()
            || !entry_header.AreSizesValid(offset, mmap->filesize())) {
          log::trace("Compaction()",
                    "Unexpected end of file - IsOK:%d, offset:%u, size_key:%" PRIu64 ", size_value_offset:%" PRIu64 ", mmap->filesize():%d\n",
                    s.IsOK(),
                    offset,
                    entry_header.size_key,
                    entry_header.size_value_offset(),
                    mmap->filesize());
          entry_header.print();
          break;
        }

        // TODO-19: make function to get location from fileid and offset, and the
        //          fileid and offset from location
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        uint64_t location = fileid_shifted | offset;

        log::trace("Compaction()", "order list loop - check if we should keep it - fileid:%u offset:%u", fileid, offset);
        if (   locations_delete.find(location) != locations_delete.end()
            || locations_secondary.find(location) != locations_secondary.end()) {
          offset += size_header + entry_header.size_key + entry_header.size_value_offset();
          continue;
        }
 
        std::vector<uint64_t> locations;
        if (hashedkeys_clusters.find(location) == hashedkeys_clusters.end()) {
          log::trace("Compaction()", "order list loop - does not have cluster");
          locations.push_back(location);
        } else {
          log::trace("Compaction()", "order list loop - has cluster of %d items", hashedkeys_clusters[location].size());
          locations = hashedkeys_clusters[location];
        }

        //for (auto it_location = locations.begin(); it_location != locations.end(); ++it_location) {
          //uint64_t location = *it_location;
        WriteOptions write_options;
        for (auto& location: locations) {
          uint32_t fileid_location = (location & 0xFFFFFFFF00000000) >> 32;
          uint32_t offset_file = location & 0x00000000FFFFFFFF;
          log::trace("Compaction()", "order list loop - location fileid:%u offset:%u", fileid_location, offset_file);
          Mmap *mmap_location = mmaps[fileid_location];
          struct EntryHeader entry_header;
          uint32_t size_header;
          Status s = EntryHeader::DecodeFrom(db_options_, mmap->datafile() + offset, mmap->filesize() - offset, &entry_header, &size_header);

          log::trace("Compaction()", "order list loop - create byte arrays");
          ByteArray key = ByteArray::NewPointerByteArray(mmap_location->datafile() + offset_file + size_header, entry_header.size_key);
          ByteArray chunk = ByteArray::NewPointerByteArray(mmap_location->datafile() + offset_file + size_header + entry_header.size_key, entry_header.size_value_used());
          log::trace("Compaction()", "order list loop - push_back() orders");

          // NOTE: Need to recompute the crc32 of the key and value, as entry_header.crc32
          //       contains information about the header, which is incorrect as the
          //       header changes due to the compaction. This could be optimized by
          //       just recomputing the crc32 of the header, and then 'uncombining'
          //       it from entry_header.crc32. This will be fixed as soon as I find an
          //       implementation of 'uncombine'.
          uint32_t crc32 = crc32c::Value(mmap->datafile() + offset + size_header, entry_header.size_key + entry_header.size_value_used());

          bool is_large = false;
          orders.push_back(Order{std::this_thread::get_id(),
                                 write_options,
                                 OrderType::Put,
                                 key,
                                 chunk,
                                 0,
                                 entry_header.size_value,
                                 entry_header.size_value_compressed,
                                 crc32,
                                 is_large});
        }
        offset += size_header + entry_header.size_key + entry_header.size_value_offset();
      }
    }
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 7. Write compacted orders on secondary storage
    log::trace("Compaction()", "Step 7: Write compacted files");
    std::multimap<uint64_t, uint64_t> map_index;
    // All the resulting files will have the same timestamp, which is the
    // maximum of all the timestamps in the set of files that have been
    // compacted. This will allow the resulting files to be properly ordered
    // during the next database startup or recovery process.
    hstable_manager_compaction_.Reset();
    hstable_manager_compaction_.LockSequenceTimestamp(timestamp_max);
    hstable_manager_compaction_.WriteOrdersAndFlushFile(orders, map_index);
    hstable_manager_compaction_.CloseCurrentFile();
    orders.clear();
    mmaps.clear();
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 8. Get fileid range from hstable_manager_
    uint32_t num_files_compacted = hstable_manager_compaction_.GetSequenceFileId();
    uint32_t offset_fileid = hstable_manager_.IncrementSequenceFileId(num_files_compacted) - num_files_compacted;
    log::trace("Compaction()", "Step 8: num_files_compacted:%u offset_fileid:%u", num_files_compacted, offset_fileid);
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 9. Rename files
    for (auto fileid = 1; fileid <= num_files_compacted; fileid++) {
      uint32_t fileid_new = fileid + offset_fileid;
      log::trace("Compaction()", "Renaming [%s] into [%s]", hstable_manager_compaction_.GetFilepath(fileid).c_str(),
                                                           hstable_manager_.GetFilepath(fileid_new).c_str());
      if (std::rename(hstable_manager_compaction_.GetFilepath(fileid).c_str(),
                      hstable_manager_.GetFilepath(fileid_new).c_str()) != 0) {
        log::emerg("Compaction()", "Could not rename file: %s", strerror(errno));
        // TODO: crash here
      }
      uint64_t filesize = hstable_manager_compaction_.file_resource_manager.GetFileSize(fileid);
      hstable_manager_.file_resource_manager.SetFileSize(fileid_new, filesize);
      hstable_manager_.file_resource_manager.SetFileCompacted(fileid_new);
    }
    if (IsStopRequested()) return Status::IOError("Stop was requested");

    
    // 10. Shift returned locations to match renamed files
    log::trace("Compaction()", "Step 10: Shifting locations");
    std::multimap<uint64_t, uint64_t> map_index_shifted;
    for (auto &p: map_index) {
      const uint64_t& hashedkey = p.first;
      const uint64_t& location = p.second;
      uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
      uint32_t offset_file = location & 0x00000000FFFFFFFF;

      uint32_t fileid_new = fileid + offset_fileid;
      uint64_t fileid_shifted = fileid_new;
      fileid_shifted <<= 32;
      uint64_t location_new = fileid_shifted | offset_file;
      log::trace("Compaction()", "Shifting [%" PRIu64 "] into [%" PRIu64 "] (fileid [%u] to [%u])", location, location_new, fileid, fileid_new);

      map_index_shifted.insert(std::pair<uint64_t, uint64_t>(hashedkey, location_new));
    }
    map_index.clear();
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 11. Add the large entries to be kept to the map that will update the 'index_'
    map_index_shifted.insert(hashedkeys_to_locations_large_keep.begin(), hashedkeys_to_locations_large_keep.end());
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 12. Update the storage engine index_, by removing the locations that have
    //     been compacted, and making sure that the locations that have been
    //     added while the compaction was taking place are not removed
    log::trace("Compaction()", "Step 12: Update the storage engine index_");
    int num_iterations_per_lock = db_options_.internal__num_iterations_per_lock;
    int counter_iterations = 0;
    for (auto it = map_index_shifted.begin(); it != map_index_shifted.end(); it = map_index_shifted.upper_bound(it->first)) {

      if (counter_iterations == 0) {
        AcquireWriteLock();
      }
      counter_iterations += 1;

      // For each hashed key, get the group of locations from the index_: all the locations
      // in that group have already been handled during the compaction, except for the ones
      // that have fileids larger than the max fileid 'fileid_end_actual' -- call these 'locations_after'.
      const uint64_t& hashedkey = it->first;
      auto range_index = index_.equal_range(hashedkey);
      std::vector<uint64_t> locations_after;
      for (auto it_bucket = range_index.first; it_bucket != range_index.second; ++it_bucket) {
        const uint64_t& location = it_bucket->second;
        uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
        if (fileid > fileid_end_actual) {
          // Save all the locations for files with fileid that were not part of
          // the compaction process
          locations_after.push_back(location);
        }
      }

      // Erase the bucket, insert the locations from the compaction process, and
      // then insert the locations from the files that were not part of the
      // compaction process, 'locations_after'
      index_.erase(hashedkey);
      auto range_compaction = map_index_shifted.equal_range(hashedkey);
      index_.insert(range_compaction.first, range_compaction.second);
      for (auto p = locations_after.begin(); p != locations_after.end(); ++p) {
        index_.insert(std::pair<uint64_t, uint64_t>(hashedkey, *p));
      }

      // Throttling the index updates, and allows other processes
      // to acquire the write lock if they need it
      if (counter_iterations >= num_iterations_per_lock) {
        ReleaseWriteLock();
        counter_iterations = 0;
      }
    }
    if (counter_iterations) ReleaseWriteLock();
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 13. Put all the locations inserted after the compaction started
    //     stored in 'index_compaction_' into the main index 'index_'
    log::trace("Compaction()", "Step 13: Transfer index_compaction_ into index_");
    AcquireWriteLock();
    // TODO-38: The pouring of index_compaction_ needs to be throttled just like the
    //          update of index_ above. The problem is that if the lock is acquire
    //          for a limited time only, then another thread could come in and want
    //          to write to the database as well: to which index should it write
    //          then, index_ or index_compaction_? More concerning, if it writes to 
    //          index_compaction_, this would mean that it would be writing to
    //          index_compaction_ as an iterator is going over index_compaction_,
    //          which would be just plain wrong. This problem will require more
    //          thinking, for now, just lock for longer and risk to cause timeouts:
    //          better be late than buggy.
    index_.insert(index_compaction_.begin(), index_compaction_.end()); 
    mutex_compaction_.lock();
    is_compaction_in_progress_ = false;
    mutex_compaction_.unlock();
    ReleaseWriteLock();
    index_compaction_.clear();
    if (IsStopRequested()) return Status::IOError("Stop was requested");


    // 14. Delete compacted files
    log::trace("Compaction()", "Step 14: Delete compacted files");
    mutex_snapshot_.lock();
    if (snapshotids_to_fileids_.size() == 0) {
      // No snapshots are in progress, remove the files on the spot
      for (auto& fileid: fileids_compaction) {
        if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
        log::trace("Compaction()", "Removing [%s]", hstable_manager_.GetFilepath(fileid).c_str());
        // TODO: free memory associated with the removed file in the file resource manager
        if (std::remove(hstable_manager_.GetFilepath(fileid).c_str()) != 0) {
          log::emerg("Compaction()", "Could not remove file [%s]", hstable_manager_.GetFilepath(fileid).c_str());
        }
        hstable_manager_.file_resource_manager.ClearAllDataForFileId(fileid);
      }
    } else {
      // Snapshots are in progress, therefore mark the files and they will be removed when the snapshots are released
      int num_snapshots = snapshotids_to_fileids_.size();
      for (auto& fileid: fileids_compaction) {
        if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
        for (auto& p: snapshotids_to_fileids_) {
          snapshotids_to_fileids_[p.first].insert(fileid);
        }
        if (num_references_to_unused_files_.find(fileid) == num_references_to_unused_files_.end()) {
          num_references_to_unused_files_[fileid] = 0;
        }
        num_references_to_unused_files_[fileid] += num_snapshots;

        // Create lock file
        std::string filepath_lock = hstable_manager_.GetLockFilepath(fileid);
        int fd;
        if ((fd = open(filepath_lock.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
          log::emerg("StorageEngine::Compaction()", "Could not open file [%s]: %s", filepath_lock.c_str(), strerror(errno));
        }
        close(fd);
      }
    }
    mutex_snapshot_.unlock();
    if (IsStopRequested()) return Status::IOError("Stop was requested");

    // Cleanup pre-allocated files
    FileUtil::remove_files_with_prefix(dbname.c_str(), prefix_compaction_);
 
    return Status::OK();
  }

  // START: Helpers for Snapshots
  // Caller must delete fileids_ignore
  Status GetNewSnapshotData(uint32_t *snapshot_id, std::set<uint32_t> **fileids_ignore) {
    std::unique_lock<std::mutex> lock(mutex_snapshot_);
    *snapshot_id = IncrementSequenceSnapshot(1);
    *fileids_ignore = new std::set<uint32_t>();
    for (auto& p: num_references_to_unused_files_) {
      (*fileids_ignore)->insert(p.first);
    }
    return Status::OK();
  }

  Status ReleaseSnapshot(uint32_t snapshot_id) {
    std::unique_lock<std::mutex> lock(mutex_snapshot_);
    if (snapshotids_to_fileids_.find(snapshot_id) == snapshotids_to_fileids_.end()) {
      return Status::IOError("No snapshot with specified id");
    }

    for (auto& fileid: snapshotids_to_fileids_[snapshot_id]) {
      if(num_references_to_unused_files_[fileid] == 1) {
        log::trace("ReleaseSnapshot()", "Removing [%s]", hstable_manager_.GetFilepath(fileid).c_str());
        if (std::remove(hstable_manager_.GetFilepath(fileid).c_str()) != 0) {
          log::emerg("ReleaseSnapshot()", "Could not remove file [%s]", hstable_manager_.GetFilepath(fileid).c_str());
        }
        if (std::remove(hstable_manager_.GetLockFilepath(fileid).c_str()) != 0) {
          log::emerg("ReleaseSnapshot()", "Could not lock file [%s]", hstable_manager_.GetLockFilepath(fileid).c_str());
        }
        hstable_manager_.file_resource_manager.ClearAllDataForFileId(fileid);
        num_references_to_unused_files_.erase(fileid);
      } else {
        num_references_to_unused_files_[fileid] -= 1;
      }
    }

    snapshotids_to_fileids_.erase(snapshot_id);
    return Status::OK();
  }

  Status ReleaseAllSnapshots() {
    for (auto& p: snapshotids_to_fileids_) {
      Status s = ReleaseSnapshot(p.first);
      if (!s.IsOK()) return s;
    }
    return Status::OK();
  }

  uint64_t GetSequenceSnapshot() {
    std::unique_lock<std::mutex> lock(mutex_sequence_snapshot_);
    return sequence_snapshot_;
  }

  uint64_t IncrementSequenceSnapshot(uint64_t inc) {
    std::unique_lock<std::mutex> lock(mutex_sequence_snapshot_);
    sequence_snapshot_ += inc;
    return sequence_snapshot_;
  }
  
  std::string GetFilepath(uint32_t fileid) {
    return hstable_manager_.GetFilepath(fileid);
  }

  uint32_t FlushCurrentFileForSnapshot() {
    return hstable_manager_.FlushCurrentFile(1, 0);
  }

  std::vector<uint32_t>* GetFileidsIterator() {
    return fileids_iterator_;
  }
  // END: Helpers for Snapshots

 private:
  void AcquireWriteLock() {
    // Also waits for readers to finish
    // NOTE: should this be made its own templated class?
    mutex_write_.lock();
    while(true) {
      std::unique_lock<std::mutex> lock_read(mutex_read_);
      if (num_readers_ == 0) break;
      cv_read_.wait(lock_read);
    }
  }

  void ReleaseWriteLock() {
    mutex_write_.unlock();
  }

  // Options
  DatabaseOptions db_options_;
  EventManager *event_manager_;
  Hash *hash_;
  bool is_read_only_;
  std::set<uint32_t>* fileids_ignore_;
  std::string prefix_compaction_;
  std::string dirpath_locks_;

  // Data
  std::string dbname_;
  HSTableManager hstable_manager_;
  std::map<uint64_t, std::string> data_;
  std::thread thread_data_;
  std::condition_variable cv_read_;
  std::mutex mutex_read_;
  std::mutex mutex_write_;
  int num_readers_;

  // Index
  std::multimap<uint64_t, uint64_t> index_;
  std::multimap<uint64_t, uint64_t> index_compaction_;
  std::thread thread_index_;
  //std::mutex mutex_index_;

  // Compaction
  HSTableManager hstable_manager_compaction_;
  std::condition_variable cv_loop_compaction_;
  std::mutex mutex_loop_compaction_;
  std::mutex mutex_compaction_;
  bool is_compaction_in_progress_;
  std::thread thread_compaction_;
  std::map<uint32_t, uint32_t> num_references_to_unused_files_;

  // Statistics
  std::mutex mutex_statistics_;
  std::thread thread_statistics_;
  std::condition_variable cv_statistics_;
  uint64_t fs_free_space_; // in bytes

  // Snapshot
  std::mutex mutex_snapshot_;
  std::map< uint32_t, std::set<uint32_t> > snapshotids_to_fileids_;
  std::mutex mutex_sequence_snapshot_;
  uint32_t sequence_snapshot_;
  std::vector<uint32_t> *fileids_iterator_;

  // Stopping and closing
  bool stop_requested_;
  bool is_closed_;
  std::mutex mutex_close_;
};

} // namespace kdb

#endif // KINGDB_STORAGE_ENGINE_H_
