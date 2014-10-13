// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_STORAGE_ENGINE_H_
#define KINGDB_STORAGE_ENGINE_H_

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
#include "storage/resource_manager.h"
#include "storage/logfile_manager.h"


namespace kdb {

class StorageEngine {
 public:
  StorageEngine(DatabaseOptions db_options,
                std::string dbname,
                bool read_only=false, // TODO: this should be part of db_options -- sure about that? what options are stored on disk?
                std::set<uint32_t>* fileids_ignore=nullptr,
                uint32_t fileid_end=0)
      : db_options_(db_options),
        is_read_only_(read_only),
        prefix_compaction_("compaction_"),
        dirpath_locks_(dbname + "/locks"),
        logfile_manager_(db_options, dbname, "", prefix_compaction_, dirpath_locks_, kUncompactedLogType, read_only),
        logfile_manager_compaction_(db_options, dbname, prefix_compaction_, prefix_compaction_, dirpath_locks_, kCompactedLogType, read_only) {
    LOG_TRACE("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    fileids_ignore_ = fileids_ignore;
    num_readers_ = 0;
    is_compaction_in_progress_ = false;
    sequence_snapshot_ = 0;
    stop_requested_ = false;
    is_closed_ = false;
    if (!is_read_only_) {
      thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
      thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
      thread_compaction_ = std::thread(&StorageEngine::ProcessingLoopCompaction, this);
    }
    hash_ = MakeHash(db_options.hash);
    if (!is_read_only_) {
      fileids_iterator_ = nullptr;
    } else {
      fileids_iterator_ = new std::vector<uint32_t>();
    }
    Status s = logfile_manager_.LoadDatabase(dbname, index_, fileids_ignore_, fileid_end, fileids_iterator_);
    if (!s.IsOK()) {
      LOG_EMERG("StorageEngine", "Could not load database");
    }
  }

  ~StorageEngine() {}

  void Close() {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    is_closed_ = true;

    // Wait for readers to exit
    AcquireWriteLock();
    logfile_manager_.Close();
    Stop();
    ReleaseWriteLock();

    if (!is_read_only_) {
      LOG_TRACE("StorageEngine::Close()", "join start");
      EventManager::update_index.NotifyWait();
      EventManager::flush_buffer.NotifyWait();
      thread_index_.join();
      thread_data_.join();
      thread_compaction_.join();
      ReleaseAllSnapshots();
      LOG_TRACE("StorageEngine::Close()", "join end");
    }

    if (fileids_ignore_ != nullptr) {
      delete fileids_ignore_; 
    }

    if (fileids_iterator_ != nullptr) {
      delete fileids_iterator_; 
    }


    LOG_TRACE("StorageEngine::Close()", "done");
  }

  bool IsStopRequested() { return stop_requested_; }
  void Stop() { stop_requested_ = true; }


  void ProcessingLoopCompaction() {
    // TODO: have the compaction loop actually do the right thing
    std::chrono::milliseconds duration(200);
    std::chrono::milliseconds forever(100000000000000000);
    while(true) {
      struct stat info;
      if (stat("/tmp/do_compaction", &info) == 0) {
        uint32_t seq = logfile_manager_.GetSequenceFileId();
        Compaction(dbname_, 1, seq+1); 
        std::this_thread::sleep_for(forever);
      }
      if (IsStopRequested()) return;
      std::this_thread::sleep_for(duration);
    }
  }

  void ProcessingLoopData() {
    while(true) {
      // Wait for orders to process
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "start");
      std::vector<Order> orders = EventManager::flush_buffer.Wait();
      if (IsStopRequested()) return;
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "got %d orders", orders.size());

      // Process orders, and create update map for the index
      AcquireWriteLock();
      std::multimap<uint64_t, uint64_t> map_index;
      logfile_manager_.WriteOrdersAndFlushFile(orders, map_index);
      ReleaseWriteLock();

      EventManager::flush_buffer.Done();
      EventManager::update_index.StartAndBlockUntilDone(map_index);
    }
  }

  void ProcessingLoopIndex() {
    while(true) {
      LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "start");
      std::multimap<uint64_t, uint64_t> index_updates = EventManager::update_index.Wait();
      if (IsStopRequested()) return;
      LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "got index_updates");
      mutex_index_.lock();

      /*
      for (auto& p: index_updates) {
        if (p.second == 0) {
          LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "remove [%s] num_items_index [%d]", p.first.c_str(), index_.size());
          index_.erase(p.first);
        } else {
          LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "put [%s]", p.first.c_str());
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

      for (auto& p: index_updates) {
        //uint64_t hashed_key = hash_->HashFunction(p.first.c_str(), p.first.size());
        LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "hash [%llu] location [%llu]", p.first, p.second);
        index->insert(std::pair<uint64_t,uint64_t>(p.first, p.second));
      }

      /*
      for (auto& p: index_) {
        LOG_TRACE("index_", "%s: %llu", p.first.c_str(), p.second);
      }
      */

      mutex_index_.unlock();
      EventManager::update_index.Done();
      LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "done");
      int temp = 1;
      EventManager::clear_buffer.StartAndBlockUntilDone(temp);
    }
  }

  // NOTE: key_out and value_out must be deleted by the caller
  Status Get(ByteArray* key, ByteArray** value_out, uint64_t *location_out=nullptr) {
    mutex_write_.lock();
    mutex_read_.lock();
    num_readers_ += 1;
    mutex_read_.unlock();
    mutex_write_.unlock();

    bool has_compaction_index = false;
    mutex_compaction_.lock();
    has_compaction_index = is_compaction_in_progress_;
    mutex_compaction_.unlock();

    Status s = Status::NotFound("");
    if (has_compaction_index) s = GetWithIndex(index_compaction_, key, value_out, location_out);
    if (!s.IsOK()) s = GetWithIndex(index_, key, value_out, location_out);

    mutex_read_.lock();
    num_readers_ -= 1;
    LOG_TRACE("Get()", "num_readers_: %d", num_readers_);
    mutex_read_.unlock();
    cv_read_.notify_one();

    return s;
  }

  // IMPORTANT: value_out must be deleled by the caller
  Status GetWithIndex(std::multimap<uint64_t, uint64_t>& index,
                      ByteArray* key,
                      ByteArray** value_out,
                      uint64_t *location_out=nullptr) {
    std::unique_lock<std::mutex> lock(mutex_index_);
    // TODO-26: should not be locking here, instead, should store the hashed key
    // and location from the index and release the lock right away -- should not
    // be locking while calling GetEntry()
    
    LOG_TRACE("StorageEngine::GetWithIndex()", "%s", key->ToString().c_str());

    // NOTE: Since C++11, the relative ordering of elements with equivalent keys
    //       in a multimap is preserved.
    uint64_t hashed_key = hash_->HashFunction(key->data(), key->size());
    auto range = index.equal_range(hashed_key);
    auto rbegin = --range.second;
    auto rend  = --range.first;
    for (auto it = rbegin; it != rend; --it) {
      ByteArray *key_temp;
      Status s = GetEntry(it->second, &key_temp, value_out); 
      LOG_TRACE("StorageEngine::GetWithIndex()", "key:[%s] key_temp:[%s] hashed_key:[%llu] hashed_key_temp:[%llu] size_key:[%llu] size_key_temp:[%llu]", key->ToString().c_str(), key_temp->ToString().c_str(), hashed_key, it->first, key->size(), key_temp->size());
      std::string temp(key_temp->data(), key_temp->size());
      LOG_TRACE("StorageEngine::GetWithIndex()", "key_temp:[%s] size[%d]", temp.c_str(), temp.size());
      if (*key_temp == *key) {
        delete key_temp;
        if (s.IsRemoveOrder()) {
          s = Status::NotFound("Unable to find the entry in the storage engine (remove order)");
        }
        if (location_out != nullptr) *location_out = it->second;
        return s;
      }
      delete key_temp;
      delete *value_out;
    }
    LOG_TRACE("StorageEngine::GetWithIndex()", "%s - not found!", key->ToString().c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }

  // IMPORTANT: key_out and value_out must be deleted by the caller
  Status GetEntry(uint64_t location,
                  ByteArray **key_out,
                  ByteArray **value_out) {
    LOG_TRACE("StorageEngine::GetEntry()", "start");
    Status s = Status::OK();
    // TODO: check that the offset falls into the
    // size of the file, just in case a file was truncated but the index
    // still had a pointer to an entry in at an invalid location --
    // alternatively, we could just let the host program crash, to force a restart
    // which would rebuild the index properly

    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    uint64_t filesize = 0;
    // NOTE: used to be in mutex_write_ and mutex_read_ -- if crashing, put the
    //       mutexes back
    filesize = logfile_manager_.file_resource_manager.GetFileSize(fileid);

    LOG_TRACE("StorageEngine::GetEntry()", "location:%llu fileid:%u offset_file:%u filesize:%llu", location, fileid, offset_file, filesize);
    std::string filepath = logfile_manager_.GetFilepath(fileid); // TODO: optimize here

    auto key_temp = new SharedMmappedByteArray(filepath, filesize);
    auto value_temp = new SharedMmappedByteArray();
    *value_temp = *key_temp;
    // NOTE: verify that value_temp.size() is indeed filesize -- verified and
    // the size was 0: should the size of an mmapped byte array be the size of
    // the file by default?

    struct Entry entry;
    uint32_t size_header;
    s = Entry::DecodeFrom(db_options_, value_temp->datafile() + offset_file, filesize - offset_file, &entry, &size_header);
    if (!s.IsOK()) return s;

    key_temp->SetOffset(offset_file + size_header, entry.size_key);
    value_temp->SetOffset(offset_file + size_header + entry.size_key, entry.size_value);
    value_temp->SetSizeCompressed(entry.size_value_compressed);
    value_temp->SetCRC32(entry.crc32);

    uint32_t crc32_headerkey = crc32c::Value(value_temp->datafile() + offset_file + 4, size_header + entry.size_key - 4);
    value_temp->SetInitialCRC32(crc32_headerkey);

    if (!entry.IsEntryFull()) {
      LOG_EMERG("StorageEngine::GetEntry()", "Entry is not of type FULL, which is not supported");
      return Status::IOError("Entries of type not FULL are not supported");
    }

    if (entry.IsTypeRemove()) {
      s = Status::RemoveOrder();
      delete value_temp;
      value_temp = nullptr;
    }

    LOG_DEBUG("StorageEngine::GetEntry()", "mmap() out - type remove:%d", entry.IsTypeRemove());
    LOG_TRACE("StorageEngine::GetEntry()", "Sizes: key_temp:%llu value_temp:%llu filesize:%llu", key_temp->size(), value_temp->size(), filesize);

    *key_out = key_temp;
    *value_out = value_temp;
    return s;
  }

  bool IsFileLarge(uint32_t fileid) {
    return logfile_manager_.file_resource_manager.IsFileLarge(fileid);
  }

  Status Compaction(std::string dbname,
                    uint32_t fileid_start,
                    uint32_t fileid_end) {
    // TODO: make sure that all sets, maps and multimaps are cleared whenever
    // they are no longer needed
    
    // TODO: when compaction starts, open() a file and lseek() to reserve disk
    //       space -- or write a bunch of files with the "compaction_" prefix
    //       that will be overwritten when the compacted files are written.

    // TODO: add a new flag in files that says "compacted" or "log", and before
    //       starting any compaction process, select only log files, ignore
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


    // 1. Get the files needed for compaction
    // TODO: This is a quick hack to get the files for compaction, by going
    //       through all the files. Fix that to be only the latest non-handled
    //       log files
    LOG_TRACE("Compaction()", "Step 1: Get files between fileids %u and %u", fileid_start, fileid_end);
    std::multimap<uint64_t, uint64_t> index_compaction;
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }
    char filepath[2048];
    uint32_t fileid = 0;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      sprintf(filepath, "%s/%s", dbname.c_str(), entry->d_name);
      fileid = LogfileManager::hex_to_num(entry->d_name);
      if (   logfile_manager_.file_resource_manager.IsFileCompacted(fileid)
          || stat(filepath, &info) != 0
          || !(info.st_mode & S_IFREG) 
          || fileid < fileid_start
          || fileid > fileid_end
          || info.st_size <= SIZE_LOGFILE_HEADER) {
        continue;
      }
      // NOTE: Here the locations are read directly from the secondary storage,
      //       which could be optimized by reading them from the index in memory. 
      //       One way to do that is to have a temporary index to which all
      //       updates are synced during compaction. That way, the main index is
      //       guaranteed to not be changed, thus all sorts of scans and changes
      //       can be done on it. Once compaction is over, the temporary index
      //       can just be poured into the main index.
      Mmap mmap(filepath, info.st_size);
      s = logfile_manager_.LoadFile(mmap, fileid, index_compaction);
      if (!s.IsOK()) {
        LOG_WARN("LogfileManager::Compaction()", "Could not load index in file [%s]", filepath);
        // TODO: handle the case where a file is found to be damaged during compaction
      }
    }
    closedir(directory);


    // 2. Iterating over all unique hashed keys of index_compaction, and determine which
    // locations of the storage engine index 'index_' with similar hashes will need to be compacted.
    LOG_TRACE("Compaction()", "Step 2: Get unique hashed keys");
    std::vector<std::pair<uint64_t, uint64_t>> index_compaction_se;
    for (auto it = index_compaction.begin(); it != index_compaction.end(); it = index_compaction.upper_bound(it->first)) {
      auto range = index_.equal_range(it->first);
      for (auto it_se = range.first; it_se != range.second; ++it_se) {
        index_compaction_se.push_back(*it_se);
      }
    }
    index_compaction.clear(); // no longer needed


    // 3. For each entry, determine which location has to be kept, which has to be deleted,
    // and the overall set of file ids that needs to be compacted
    LOG_TRACE("Compaction()", "Step 3: Determine locations");
    std::set<uint64_t> locations_delete;
    std::set<uint32_t> fileids_compaction;
    std::set<uint32_t> fileids_largefiles_keep;
    std::set<std::string> keys_encountered;
    std::multimap<uint64_t, uint64_t> hashedkeys_to_locations_regular_keep;
    std::multimap<uint64_t, uint64_t> hashedkeys_to_locations_large_keep;
    // Reversing the order of the vector to guarantee that
    // the most recent locations are treated first
    std::reverse(index_compaction_se.begin(), index_compaction_se.end());
    for (auto &p: index_compaction_se) {
      ByteArray *key, *value;
      uint64_t& location = p.second;
      uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
      if (fileid > fileid_end) {
        // Make sure that files added after the compacted
        // log files or during the compaction itself are not used
        continue;
      }
      fileids_compaction.insert(fileid);
      Status s = GetEntry(location, &key, &value);
      std::string str_key = key->ToString();
      delete key;
      delete value;

      // For any given key, only the first occurrence, which is the most recent one,
      // has to be kept. The other ones will be deleted. If the first occurrence
      // is a Remove Order, then all occurrences of that key will be deleted.
      if (keys_encountered.find(str_key) == keys_encountered.end()) {
        keys_encountered.insert(str_key);
        if (IsFileLarge(fileid)) {
          hashedkeys_to_locations_large_keep.insert(p);
          fileids_largefiles_keep.insert(fileid);
        } else if (!s.IsRemoveOrder()) {
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


    // 4. Building the clusters of locations, indexed by the smallest location
    // per cluster. All the non-smallest locations are stored as secondary
    // locations. Only regular entries are used: it would not make sense
    // to compact large entries anyway.
    LOG_TRACE("Compaction()", "Step 4: Building clusters");
    std::map<uint64_t, std::vector<uint64_t>> hashedkeys_clusters;
    std::set<uint64_t> locations_secondary;
    for (auto it = hashedkeys_to_locations_regular_keep.begin(); it != hashedkeys_to_locations_regular_keep.end(); it = hashedkeys_to_locations_regular_keep.upper_bound(it->first)) {
      auto range = hashedkeys_to_locations_regular_keep.equal_range(it->first);
      std::vector<uint64_t> locations;
      for (auto it_bucket = range.first; it_bucket != range.second; ++it_bucket) {
        LOG_TRACE("Compaction()", "Building clusters - location:%llu", it->second);
        locations.push_back(it->second);
      }
      std::sort(locations.begin(), locations.end());
      hashedkeys_clusters[locations[0]] = locations;
      for (auto i = 1; i < locations.size(); i++) {
        locations_secondary.insert(locations[i]);
      }
    }
    hashedkeys_to_locations_regular_keep.clear();

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

    // 5. Mmapping all the files involved in the compaction
    LOG_TRACE("Compaction()", "Step 5: Mmap() all the files! ALL THE FILES!");
    std::map<uint32_t, Mmap*> mmaps;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
      struct stat info;
      std::string filepath = logfile_manager_.GetFilepath(fileid);
      if (stat(filepath.c_str(), &info) != 0 || !(info.st_mode & S_IFREG)) {
        LOG_EMERG("Compaction()", "Error during compaction with file [%s]", filepath.c_str());
      }
      Mmap *mmap = new Mmap(filepath.c_str(), info.st_size);
      mmaps[fileid] = mmap;
    }


    // 6. Now building a vector of orders, that will be passed to the
    //    logmanager_compaction_ object to persist them on disk
    LOG_TRACE("Compaction()", "Step 6: Build order list");
    std::vector<Order> orders;
    uint64_t timestamp_max = 0;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (IsFileLarge(fileid)) continue;
      Mmap* mmap = mmaps[fileid];

      // Read the header to update the maximimum timestamp
      struct LogFileHeader lfh;
      s = LogFileHeader::DecodeFrom(mmap->datafile(), mmap->filesize(), &lfh);
      if (!s.IsOK()) return Status::IOError("Could not read file header during compaction"); // TODO: skip file instead of returning an error 
      timestamp_max = std::max(timestamp_max, lfh.timestamp);

      // Read the footer to get the offset where entries stop
      struct LogFileFooter footer;
      Status s = LogFileFooter::DecodeFrom(mmap->datafile() + mmap->filesize() - LogFileFooter::GetFixedSize(), LogFileFooter::GetFixedSize(), &footer);
      uint32_t crc32_computed = crc32c::Value(mmap->datafile() + footer.offset_indexes, mmap->filesize() - footer.offset_indexes - 4);
      uint64_t offset_end;
      if (   !s.IsOK()
          || footer.magic_number != LogfileManager::get_magic_number()
          || footer.crc32 != crc32_computed) {
        // TODO: handle error
        offset_end = mmap->filesize();
        LOG_TRACE("Compaction()", "Compaction - invalid footer");
      } else {
        offset_end = footer.offset_indexes;
      }

      // Process entries in the file
      uint32_t offset = SIZE_LOGFILE_HEADER;
      while (offset < offset_end) {
        LOG_TRACE("Compaction()", "order list loop - offset:%u offset_end:%u", offset, offset_end);
        struct Entry entry;
        uint32_t size_header;
        Status s = Entry::DecodeFrom(db_options_, mmap->datafile() + offset, mmap->filesize() - offset, &entry, &size_header);
        // NOTE: The checksum is not verified because during the compaction it
        // doesn't matter whether or not the entry is valid. The user will know
        // that an entry is invalid after doing a Get(), and that his choice to
        // emit a 'delete' command if he wants to delete the entry.
        
        // NOTE: the uses of sizeof(struct Entry) here make not sense, since this
        // size is variable based on the local architecture
        if (   !s.IsOK()
            || offset + sizeof(struct Entry) >= mmap->filesize()
            || entry.size_key == 0
            || offset + sizeof(struct Entry) + entry.size_key > mmap->filesize()
            || offset + sizeof(struct Entry) + entry.size_key + entry.size_value_offset() > mmap->filesize()) {
          LOG_TRACE("Compaction()", "Unexpected end of file - mmap->filesize():%d\n", mmap->filesize());
          entry.print();
          break;
        }

        // TODO-19: make function to get location from fileid and offset, and the
        //          fileid and offset from location
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        uint64_t location = fileid_shifted | offset;

        LOG_TRACE("Compaction()", "order list loop - check if we should keep it - fileid:%u offset:%u", fileid, offset);
        if (   locations_delete.find(location) != locations_delete.end()
            || locations_secondary.find(location) != locations_secondary.end()) {
          offset += size_header + entry.size_key + entry.size_value_offset();
          continue;
        }
 
        std::vector<uint64_t> locations;
        if (hashedkeys_clusters.find(location) == hashedkeys_clusters.end()) {
          LOG_TRACE("Compaction()", "order list loop - does not have cluster");
          locations.push_back(location);
        } else {
          LOG_TRACE("Compaction()", "order list loop - has cluster of %d items", hashedkeys_clusters[location].size());
          locations = hashedkeys_clusters[location];
        }

        //for (auto it_location = locations.begin(); it_location != locations.end(); ++it_location) {
          //uint64_t location = *it_location;
        for (auto& location: locations) {
          uint32_t fileid_location = (location & 0xFFFFFFFF00000000) >> 32;
          uint32_t offset_file = location & 0x00000000FFFFFFFF;
          LOG_TRACE("Compaction()", "order list loop - location fileid:%u offset:%u", fileid_location, offset_file);
          Mmap *mmap_location = mmaps[fileid_location];
          struct Entry entry;
          uint32_t size_header;
          Status s = Entry::DecodeFrom(db_options_, mmap->datafile() + offset, mmap->filesize() - offset, &entry, &size_header);

          LOG_TRACE("Compaction()", "order list loop - create byte arrays");
          ByteArray *key   = new SimpleByteArray(mmap_location->datafile() + offset_file + size_header, entry.size_key);
          ByteArray *chunk = new SimpleByteArray(mmap_location->datafile() + offset_file + size_header + entry.size_key, entry.size_value_used());
          LOG_TRACE("Compaction()", "order list loop - push_back() orders");
          orders.push_back(Order{std::this_thread::get_id(),
                                 OrderType::Put,
                                 key,
                                 chunk,
                                 0,
                                 entry.size_value,
                                 entry.size_value_compressed,
                                 entry.crc32});
        }
        offset += size_header + entry.size_key + entry.size_value_offset();
      }
    }


    // 7. Write compacted orders on secondary storage
    LOG_TRACE("Compaction()", "Step 7: Write compacted files");
    std::multimap<uint64_t, uint64_t> map_index;
    // All the resulting files will have the same timestamp, which is the
    // maximum of all the timestamps in the set of files that have been
    // compacted. This will allow the resulting files to be properly ordered
    // during the next database startup or recovery process.
    logfile_manager_compaction_.LockSequenceTimestamp(timestamp_max);
    logfile_manager_compaction_.WriteOrdersAndFlushFile(orders, map_index);
    logfile_manager_compaction_.CloseCurrentFile();
    orders.clear();
    mmaps.clear();


    // 8. Get fileid range from logfile_manager_
    uint32_t num_files_compacted = logfile_manager_compaction_.GetSequenceFileId();
    uint32_t offset_fileid = logfile_manager_.IncrementSequenceFileId(num_files_compacted) - num_files_compacted;
    LOG_TRACE("Compaction()", "Step 8: num_files_compacted:%u offset_fileid:%u", num_files_compacted, offset_fileid);


    // 9. Rename files
    for (auto fileid = 1; fileid <= num_files_compacted; fileid++) {
      uint32_t fileid_new = fileid + offset_fileid;
      LOG_TRACE("Compaction()", "Renaming [%s] into [%s]", logfile_manager_compaction_.GetFilepath(fileid).c_str(),
                                                           logfile_manager_.GetFilepath(fileid_new).c_str());
      if (std::rename(logfile_manager_compaction_.GetFilepath(fileid).c_str(),
                      logfile_manager_.GetFilepath(fileid_new).c_str()) != 0) {
        LOG_EMERG("Compaction()", "Could not rename file");
        // TODO: crash here
      }
      uint64_t filesize = logfile_manager_compaction_.file_resource_manager.GetFileSize(fileid);
      logfile_manager_.file_resource_manager.SetFileSize(fileid_new, filesize);
      logfile_manager_.file_resource_manager.SetFileCompacted(fileid_new);
    }

    
    // 10. Shift returned locations to match renamed files
    LOG_TRACE("Compaction()", "Step 10: Shifting locations");
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
      LOG_TRACE("Compaction()", "Shifting [%llu] into [%llu] (fileid [%u] to [%u])", location, location_new, fileid, fileid_new);

      map_index_shifted.insert(std::pair<uint64_t, uint64_t>(hashedkey, location_new));
    }
    map_index.clear();


    // 11. Add the large entries to be kept to the map that will update the 'index_'
    map_index_shifted.insert(hashedkeys_to_locations_large_keep.begin(), hashedkeys_to_locations_large_keep.end());


    // 12. Update the storage engine index_, by removing the locations that have
    //     been compacted, while making sure that the locations that have been
    //     added as the compaction are not removed
    LOG_TRACE("Compaction()", "Step 12: Update the storage engine index_");
    int num_iterations_per_lock = 10;
    int counter_iterations = 0;
    for (auto it = map_index_shifted.begin(); it != map_index_shifted.end(); it = map_index_shifted.upper_bound(it->first)) {

      if (counter_iterations == 0) {
        AcquireWriteLock();
      }
      counter_iterations += 1;

      // For each hashed key, get the group of locations from the index_: all the locations
      // in that group have already been handled during the compaction, except for the ones
      // that have fileids larger than the max fileid 'fileid_end' -- call these 'locations_after'.
      const uint64_t& hashedkey = it->first;
      auto range_index = index_.equal_range(hashedkey);
      std::vector<uint64_t> locations_after;
      for (auto it_bucket = range_index.first; it_bucket != range_index.second; ++it_bucket) {
        const uint64_t& location = it_bucket->second;
        uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
        if (fileid > fileid_end) {
          // Save all the locations for files with fileid that were not part of
          // the compaction process
          locations_after.push_back(location);
        }
      }

      // Erase the bucket, insert the locations from the compaction process, and
      // then insert the locations from the files that were not part of the
      // compaction process started, 'locations_after'
      index_.erase(hashedkey);
      auto range_compaction = map_index_shifted.equal_range(hashedkey);
      index_.insert(range_compaction.first, range_compaction.second);
      for (auto p = locations_after.begin(); p != locations_after.end(); ++p) {
        index_.insert(std::pair<uint64_t, uint64_t>(hashedkey, *p));
      }

      // Release the lock if needed (throttling)
      if (counter_iterations >= num_iterations_per_lock) {
        ReleaseWriteLock();
        counter_iterations = 0;
      }
    }
    ReleaseWriteLock();


    // 13. Put all the locations inserted after the compaction started
    //     stored in 'index_compaction_' into the main index 'index_'
    LOG_TRACE("Compaction()", "Step 13: Transfer index_compaction_ into index_");
    AcquireWriteLock();
    index_.insert(index_compaction_.begin(), index_compaction_.end()); 
    index_compaction_.clear();
    mutex_compaction_.lock();
    is_compaction_in_progress_ = false;
    mutex_compaction_.unlock();
    ReleaseWriteLock();


    // 14. Remove compacted files
    LOG_TRACE("Compaction()", "Step 14: Remove compacted files");
    mutex_snapshot_.lock();
    if (snapshotids_to_fileids_.size() == 0) {
      // No snapshots are in progress, remove the files on the spot
      for (auto& fileid: fileids_compaction) {
        if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
        LOG_TRACE("Compaction()", "Removing [%s]", logfile_manager_.GetFilepath(fileid).c_str());
        // TODO: free memory associated with the removed file in the file resource manager
        if (std::remove(logfile_manager_.GetFilepath(fileid).c_str()) != 0) {
          LOG_EMERG("Compaction()", "Could not remove file [%s]", logfile_manager_.GetFilepath(fileid).c_str());
        }
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
        std::string filepath_lock = logfile_manager_.GetLockFilepath(fileid);
        int fd;
        if ((fd = open(filepath_lock.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
          LOG_EMERG("StorageEngine::Compaction()", "Could not open file [%s]: %s", filepath_lock.c_str(), strerror(errno));
        }
        close(fd);
      }
    }
    mutex_snapshot_.unlock();

    // TODO-20: update changelogs and fsync() wherever necessary (journal, or whatever name, which has
    //          the sequence of operations that can be used to recover)
 
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
        LOG_TRACE("ReleaseSnapshot()", "Removing [%s]", logfile_manager_.GetFilepath(fileid).c_str());
        if (std::remove(logfile_manager_.GetFilepath(fileid).c_str()) != 0) {
          LOG_EMERG("ReleaseSnapshot()", "Could not remove file [%s]", logfile_manager_.GetFilepath(fileid).c_str());
        }
        if (std::remove(logfile_manager_.GetLockFilepath(fileid).c_str()) != 0) {
          LOG_EMERG("ReleaseSnapshot()", "Could not lock file [%s]", logfile_manager_.GetLockFilepath(fileid).c_str());
        }
      } else {
        num_references_to_unused_files_[fileid] -= 1;
      }
    }

    snapshotids_to_fileids_.erase(snapshot_id);
    return Status::OK();
  }

  Status ReleaseAllSnapshots() {
    for (auto& p: snapshotids_to_fileids_) {
      ReleaseSnapshot(p.first);
    }
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
    return logfile_manager_.GetFilepath(fileid);
  }

  uint32_t FlushCurrentFileForSnapshot() {
    // TODO: flushing the current file is not enough, I also need to make sure
    //       that all the buffers are flushed
    return logfile_manager_.FlushCurrentFile(1, 0);
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
  Hash *hash_;
  bool is_read_only_;
  std::set<uint32_t>* fileids_ignore_;
  std::string prefix_compaction_;
  std::string dirpath_locks_;

  // Data
  std::string dbname_;
  LogfileManager logfile_manager_;
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
  std::mutex mutex_index_;

  // Compaction
  LogfileManager logfile_manager_compaction_;
  std::mutex mutex_compaction_;
  bool is_compaction_in_progress_;
  std::thread thread_compaction_;
  std::mutex mutex_fileds_compacted_;
  std::set<uint32_t> fileids_compacted_;
  std::map<uint32_t, uint32_t> num_references_to_unused_files_;

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
