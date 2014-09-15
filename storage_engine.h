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

#include "kdb.h"
#include "options.h"
#include "hash.h"
#include "common.h"
#include "byte_array.h"
#include "crc32c.h"


namespace kdb {

// TODO: split into .cc and .h files

// TODO: split the classes into their own respective files

// TODO-21: Due to padding/alignment, the structs that are used to store data in
//          files will see their size influenced by the architecture on which
//          the database is running (i.e. 32 bits or 64 bits), and thus the actual
//          storage will need serialization (along with proper endian-ness
//          handling)

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
    return (largesfiles_.find(fileid) != largesfiles_.end());
  }

  void SetFileLarge(uint32_t fileid) {
    std::unique_lock<std::mutex> lock(mutex_);
    largesfiles_.insert(fileid);
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
  std::set<uint32_t> largesfiles_;
  std::map<uint32_t, uint64_t> num_writes_in_progress_;
  std::map<uint32_t, std::vector< std::pair<uint64_t, uint32_t> > > logindexes_;
  std::set<uint32_t> has_padding_in_values_;
};


class LogfileManager {
 public:
  LogfileManager(DatabaseOptions& db_options, std::string dbname, std::string prefix)
      : db_options_(db_options),
        prefix_(prefix) {
    LOG_TRACE("LogfileManager::LogfileManager()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    sequence_fileid_ = 1;
    sequence_timestamp_ = 1;
    size_block_ = SIZE_LOGFILE_TOTAL;
    has_file_ = false;
    buffer_has_items_ = false;
    buffer_raw_ = new char[size_block_*2];
    buffer_index_ = new char[size_block_*2];
    hash_ = MakeHash(db_options.hash);
    is_closed_ = false;
    is_locked_sequence_timestamp_ = false;
  }

  ~LogfileManager() {
    Close();
  }

  void Close() {
    if (!is_closed_) {
      is_closed_ = true;
      FlushCurrentFile();
      CloseCurrentFile();
      delete[] buffer_raw_;
      delete[] buffer_index_;
    }
  }

  std::string GetPrefix() {
    return prefix_;
  }

  std::string GetFilepath(uint32_t fileid) {
    //filepath_ = dbname_ + "/" + std::to_string(sequence_fileid_); // TODO: optimize here
    return dbname_ + "/" + prefix_ + LogfileManager::num_to_hex(fileid); // TODO: optimize here
  }

  // File id sequence helpers
  void SetSequenceFileId(uint32_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    sequence_fileid_ = seq;
  }

  uint32_t GetSequenceFileId() {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    return sequence_fileid_;
  }

  uint32_t IncrementSequenceFileId(uint32_t inc) {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    sequence_fileid_ += inc;
    return sequence_fileid_;
  }


  // Timestamp sequence helpers
  void SetSequenceTimestamp(uint32_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    if (!is_locked_sequence_timestamp_) sequence_timestamp_ = seq;
  }

  uint64_t GetSequenceTimestamp() {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    return sequence_timestamp_;
  }

  uint64_t IncrementSequenceTimestamp(uint64_t inc) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    if (!is_locked_sequence_timestamp_) sequence_timestamp_ += inc;
    return sequence_timestamp_;
  }

  void LockSequenceTimestamp(uint64_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    is_locked_sequence_timestamp_ = true;
    sequence_timestamp_ = seq;
  }


  static std::string num_to_hex(uint64_t num) {
    char buffer[20];
    sprintf(buffer, "%08llX", num);
    return std::string(buffer);
  }
  
  static uint32_t hex_to_num(char* hex) {
    uint32_t num;
    sscanf(hex, "%x", &num);
    return num;
  }

  void OpenNewFile() {
    filepath_ = GetFilepath(GetSequenceFileId());
    if ((fd_ = open(filepath_.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      LOG_EMERG("StorageEngine::ProcessingLoopData()", "Could not open file [%s]: %s", filepath_.c_str(), strerror(errno));
      exit(-1); // TODO-3: gracefully handle open() errors
    }
    has_file_ = true;
    fileid_ = GetSequenceFileId();
    timestamp_ = GetSequenceTimestamp();

    // Reserving space for header
    offset_start_ = 0;
    offset_end_ = SIZE_LOGFILE_HEADER;

    // Filling in default header
    struct LogFileHeader* lfh = reinterpret_cast<struct LogFileHeader*>(buffer_raw_);
    lfh->filetype = kLogType;
    lfh->timestamp = timestamp_;
  }

  void CloseCurrentFile() {
    LOG_TRACE("LogfileManager::CloseCurrentFile()", "ENTER - fileid_:%d", fileid_);
    FlushLogIndex();
    close(fd_);
    IncrementSequenceFileId(1);
    IncrementSequenceTimestamp(1);
    buffer_has_items_ = false;
    has_file_ = false;
  }

  void FlushCurrentFile(int force_new_file=0, uint64_t padding=0) {
    LOG_TRACE("LogfileManager::FlushCurrentFile()", "ENTER - fileid_:%d, has_file_:%d, buffer_has_items_:%d", fileid_, has_file_, buffer_has_items_);
    if (has_file_ && buffer_has_items_) {
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "has_files && buffer_has_items_ - fileid_:%d", fileid_);
      if (write(fd_, buffer_raw_ + offset_start_, offset_end_ - offset_start_) < 0) {
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "Error write(): %s", strerror(errno));
      }
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      offset_start_ = offset_end_;
      buffer_has_items_ = false;
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "items written - offset_end_:%d | size_block_:%d | force_new_file:%d", offset_end_, size_block_, force_new_file);
    }

    if (padding) {
      offset_end_ += padding;
      offset_start_ = offset_end_;
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      ftruncate(fd_, offset_end_);
      lseek(fd_, 0, SEEK_END);
    }

    if (offset_end_ >= size_block_ || (force_new_file && offset_end_ > SIZE_LOGFILE_HEADER)) {
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "file renewed - force_new_file:%d", force_new_file);
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      CloseCurrentFile();
      OpenNewFile();
    }
    LOG_TRACE("LogfileManager::FlushCurrentFile()", "done!");
  }


  Status FlushLogIndex() {
    uint64_t num = file_resource_manager.GetNumWritesInProgress(fileid_);
    LOG_TRACE("LogfileManager::FlushLogIndex()", "ENTER - fileid_:%d - num_writes_in_progress:%llu", fileid_, num);
    if (file_resource_manager.GetNumWritesInProgress(fileid_) == 0) {
      uint64_t size_logindex;
      Status s = WriteLogIndex(fd_, file_resource_manager.GetLogIndex(fileid_), &size_logindex, kLogType, file_resource_manager.HasPaddingInValues(fileid_), false);
      uint64_t filesize = file_resource_manager.GetFileSize(fileid_);
      file_resource_manager.SetFileSize(fileid_, filesize + size_logindex);
      return s;
    }
    return Status::OK();
  }


  Status WriteLogIndex(int fd,
                       const std::vector< std::pair<uint64_t, uint32_t> >& logindex_current,
                       uint64_t* size_out,
                       FileType filetype,
                       bool has_padding_in_values,
                       bool has_invalid_entries) {
    uint64_t offset = 0;
    for (auto& p: logindex_current) {
      auto item = reinterpret_cast<struct LogFileFooterIndex*>(buffer_index_ + offset);
      item->hashed_key = p.first;
      item->offset_entry = p.second;
      //memcpy(buffer_index_ + offset, &(p.first), sizeof(p.first));
      //memcpy(buffer_index_ + offset + sizeof(p.first), &(p.second), sizeof(p.second));
      offset += sizeof(struct LogFileFooterIndex);//sizeof(p.first) + sizeof(p.second);
      LOG_TRACE("StorageEngine::WriteLogIndex()", "hashed_key:[%llu] offset:[%u] offset_hex:[%s]", p.first, p.second, num_to_hex(p.second).c_str());
    }
    struct LogFileFooter* footer = reinterpret_cast<struct LogFileFooter*>(buffer_index_ + offset);
    footer->filetype = filetype;
    footer->num_entries = logindex_current.size();
    footer->magic_number = get_magic_number();
    footer->has_padding_in_values = has_padding_in_values;
    footer->has_invalid_entries = has_invalid_entries;
    offset += sizeof(struct LogFileFooter);
    lseek(fd, 0, SEEK_END);
    if (write(fd, buffer_index_, offset) < 0) {
      LOG_TRACE("StorageEngine::WriteLogIndex()", "Error write(): %s", strerror(errno));
    }
    *size_out = offset;
    LOG_TRACE("StorageEngine::WriteLogIndex()", "num_entries:[%lu]", logindex_current.size());
    return Status::OK();
  }


  uint64_t WriteFirstChunkLargeOrder(Order& order, uint64_t hashed_key) {
    // TODO: what if the large order is self-contained? then need to do all the
    // actions done for the last chunk in WriteChunk() -- maybe make a new
    // method to factorize that code
    uint64_t fileid_largefile = IncrementSequenceFileId(1);
    uint64_t timestamp_largefile = IncrementSequenceTimestamp(1);
    std::string filepath = GetFilepath(fileid_largefile);
    LOG_TRACE("LogfileManager::WriteFirstChunkLargeOrder()", "enter %s", filepath.c_str());
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      LOG_EMERG("StorageEngine::WriteFirstChunkLargeOrder()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      exit(-1); // TODO-3: gracefully handle open() errors
    }

    char buffer[SIZE_LOGFILE_HEADER];

    // Write header
    struct LogFileHeader* lfh = reinterpret_cast<struct LogFileHeader*>(buffer);
    lfh->filetype = kLargeType;
    lfh->timestamp = timestamp_largefile;
    if(write(fd, buffer, SIZE_LOGFILE_HEADER) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }

    // Write entry metadata
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer);
    entry->SetTypePut();
    entry->SetEntryFull();
    entry->size_key = order.key->size();
    entry->size_value = order.size_value;
    entry->size_value_compressed = order.size_value_compressed;
    entry->hash = hashed_key;
    entry->crc32 = 0;
    entry->SetHasPadding(false);
    if(write(fd, buffer, sizeof(struct Entry)) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }

    // Write key and chunk
    if(write(fd, order.key->data(), order.key->size()) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, order.chunk->data(), order.chunk->size()) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }

    uint64_t filesize = SIZE_LOGFILE_HEADER + sizeof(struct Entry) + order.key->size() + order.size_value;
    ftruncate(fd, filesize);
    file_resource_manager.SetFileSize(fileid_largefile, filesize);
    close(fd);
    uint64_t fileid_shifted = fileid_largefile;
    fileid_shifted <<= 32;
    LOG_TRACE("LogfileManager::WriteFirstChunkLargeOrder()", "fileid [%d]", fileid_largefile);
    file_resource_manager.SetNumWritesInProgress(fileid_largefile, 1);
    return fileid_shifted | SIZE_LOGFILE_HEADER;
  }


  uint64_t WriteChunk(Order& order, uint64_t hashed_key, uint64_t location, bool is_large_order) {
    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    std::string filepath = GetFilepath(fileid);
    LOG_TRACE("LogfileManager::WriteChunk()", "key [%s] filepath:[%s] offset_chunk:%llu", order.key->ToString().c_str(), filepath.c_str(), order.offset_chunk);
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY, 0644)) < 0) {
      LOG_EMERG("StorageEngine::WriteChunk()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      exit(-1); // TODO-3: gracefully handle open() errors
    }

    // Write the chunk
    if (pwrite(fd,
               order.chunk->data(),
               order.chunk->size(),
               offset_file + sizeof(struct Entry) + order.key->size() + order.offset_chunk) < 0) {
      LOG_TRACE("LogfileManager::WriteChunk()", "Error pwrite(): %s", strerror(errno));
    }

    // If this is a last chunk, the header is written again to save the right size of compressed value,
    // and the crc32 is saved too
    if (order.IsLastChunk()) {
      LOG_TRACE("LogfileManager::WriteChunk()", "Write compressed size: [%s] - size:%llu, compressed size:%llu crc32:%u", order.key->ToString().c_str(), order.size_value, order.size_value_compressed, order.crc32);
      struct Entry entry;
      entry.SetTypePut();
      entry.SetEntryFull();
      entry.size_key = order.key->size();
      entry.size_value = order.size_value;
      entry.size_value_compressed = order.size_value_compressed;
      if (!is_large_order && entry.IsCompressed()) {
        entry.SetHasPadding(true);
        file_resource_manager.SetHasPaddingInValues(fileid_, true);
      }
      entry.hash = hashed_key;
      entry.crc32 = order.crc32;
      if (pwrite(fd, &entry, sizeof(struct Entry), offset_file) < 0) {
        LOG_TRACE("LogfileManager::WriteChunk()", "Error pwrite(): %s", strerror(errno));
      }
      
      if (is_large_order && entry.IsCompressed()) {
        uint64_t filesize = SIZE_LOGFILE_HEADER + sizeof(struct Entry) + order.key->size() + order.size_value_compressed;
        file_resource_manager.SetFileSize(fileid, filesize);
        ftruncate(fd, filesize);
      }

      uint32_t num_writes = file_resource_manager.SetNumWritesInProgress(fileid, -1);
      if (num_writes == 0) {
        lseek(fd, 0, SEEK_END);
        uint64_t size_logindex;
        FileType filetype = is_large_order ? kLargeType : kLogType;
        WriteLogIndex(fd, file_resource_manager.GetLogIndex(fileid), &size_logindex, filetype, file_resource_manager.HasPaddingInValues(fileid), false);
        uint64_t filesize = file_resource_manager.GetFileSize(fileid);
        filesize += size_logindex;
        file_resource_manager.SetFileSize(fileid, filesize);
        if (is_large_order) file_resource_manager.SetFileLarge(fileid);
        file_resource_manager.ResetDataForFileId(fileid);
      }

    }

    close(fd);
    LOG_TRACE("LogfileManager::WriteChunk()", "all good");
    return location;
  }


  uint64_t WriteFirstChunkOrSmallOrder(Order& order, uint64_t hashed_key) {
    uint64_t location_out = 0;
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer_raw_ + offset_end_);
    if (order.type == OrderType::Put) {
      entry->SetTypePut();
      entry->SetEntryFull();
      entry->size_key = order.key->size();
      entry->size_value = order.size_value;
      entry->size_value_compressed = order.size_value_compressed;
      entry->hash = hashed_key;
      entry->crc32 = order.crc32;
      if (order.IsSelfContained()) {
        entry->SetHasPadding(false);
      } else {
        entry->SetHasPadding(true);
        file_resource_manager.SetHasPaddingInValues(fileid_, true);
      }
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry), order.key->data(), order.key->size());
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry) + order.key->size(), order.chunk->data(), order.chunk->size());

      //map_index[order.key] = fileid_ | offset_end_;
      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      file_resource_manager.AddLogIndex(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      offset_end_ += sizeof(struct Entry) + order.key->size() + order.chunk->size();

      if (!order.IsSelfContained()) {
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "BEFORE fileid_ %u", fileid_);
        file_resource_manager.SetNumWritesInProgress(fileid_, 1);
        FlushCurrentFile(0, order.size_value - order.chunk->size());
        // NOTE: A better way to do it would be to copy things into the buffer, and
        // then for the other chunks, either copy in the buffer if the position
        // to write is >= offset_end_, or do a pwrite() if the position is <
        // offset_end_
        // NOTE: might be better to lseek() instead of doing a large write
        // NOTE: No longer necessary to do the lseek() here, as I'm doing it in
        // the FlushCurrentFile()
        //offset_end_ += order.size_value - order.size_chunk;
        //FlushCurrentFile();
        //ftruncate(fd_, offset_end_);
        //lseek(fd_, 0, SEEK_END);
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "AFTER fileid_ %u", fileid_);
      }
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Put [%s]", order.key->ToString().c_str());
    } else { // order.type == OrderType::Remove
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Remove [%s]", order.key->ToString().c_str());
      entry->SetTypeRemove();
      entry->SetEntryFull();
      entry->size_key = order.key->size();
      entry->size_value = 0;
      entry->size_value_compressed = 0;
      entry->crc32 = 0;
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry), order.key->data(), order.key->size());

      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      file_resource_manager.AddLogIndex(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      offset_end_ += sizeof(struct Entry) + order.key->size();
    }
    return location_out;
  }

  void WriteOrdersAndFlushFile(std::vector<Order>& orders, std::multimap<uint64_t, uint64_t>& map_index_out) {
    for (auto& order: orders) {

      if (!has_file_) OpenNewFile();

      if (offset_end_ > size_block_) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "About to flush - offset_end_: %llu | size_key: %d | size_value: %d | size_block_: %llu", offset_end_, order.key->size(), order.size_value, size_block_);
        FlushCurrentFile(true, 0);
      }

      uint64_t hashed_key = hash_->HashFunction(order.key->data(), order.key->size());
      // TODO-13: if the item is self-contained (unique chunk), then no need to
      //       have size_value space, size_value_compressed is enough.

      // TODO-12: If the db is embedded, then all order are self contained,
      //       independently of their sizes. Would the compression and CRC32 still
      //       work? Would storing the data (i.e. choosing between the different
      //       storing functions) still work?

      // NOTE: orders can be of various sizes: when using the storage engine as an
      // embedded engine, orders can be of any size, and when plugging the
      // storage engine to a network server, orders can be chucks of data.

      // 1. The order is the first chunk of a very large entry, so we
      //    create a very large file and write the first chunk in there
      uint64_t location = 0;
      bool is_large_order = order.key->size() + order.size_value > size_block_;
      // TODO: is_large_order should become part of 'struct Order'
      if (is_large_order && order.IsFirstChunk()) {
        // TODO-11: shouldn't this be testing size_value_compressed as well? -- yes, only if the order
        // is a full entry by itself (will happen when the kvstore will be embedded and not accessed
        // through the network), otherwise we don't know yet what the total compressed size will be.
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "1. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        location = WriteFirstChunkLargeOrder(order, hashed_key);
      // 2. The order is a non-first chunk, so we
      //    open the file, pwrite() the chunk, and close the file.
      } else if (order.offset_chunk != 0) {
        //  TODO-11: replace the tests on compression "order.size_value_compressed ..." by a real test on a flag or a boolean
        //  TODO-11: replace the use of size_value or size_value_compressed by a unique size() which would already return the right value
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "2. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        if (key_to_location.find(order.tid) == key_to_location.end()) {
          location = 0;
        } else {
          location = key_to_location[order.tid][order.key->ToString()];
        }
        if (location != 0) {
          WriteChunk(order, hashed_key, location, is_large_order);
        } else {
          LOG_EMERG("StorageEngine", "Avoided catastrophic location error"); 
        }

      // 3. The order is the first chunk of a small or self-contained entry
      } else {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "3. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        buffer_has_items_ = true;
        location = WriteFirstChunkOrSmallOrder(order, hashed_key);
      }

      // If the order was the self-contained or the last chunk, add his location to the output map_index_out[]
      if (order.IsSelfContained() || order.IsLastChunk()) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "END OF ORDER key: [%s] size_chunk:%llu offset_chunk: %llu location:%llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk, location);
        if (location != 0) {
          map_index_out.insert(std::pair<uint64_t, uint64_t>(hashed_key, location));
        } else {
          LOG_EMERG("StorageEngine", "Avoided catastrophic location error"); 
        }
        if (key_to_location.find(order.tid) != key_to_location.end()) {
          key_to_location[order.tid].erase(order.key->ToString());
        }
      // Else, if the order is not self-contained and is the first chunk,
      // the location is saved in key_to_location[]
      } else if (order.IsFirstChunk()) {
        if (location != 0 && order.type != OrderType::Remove) {
          key_to_location[order.tid][order.key->ToString()] = location;
        } else {
          LOG_EMERG("StorageEngine", "Avoided catastrophic location error"); 
        }
      }
    }
    LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "end flush");
    FlushCurrentFile(0, 0);
  }


  Status LoadDatabase(std::string& dbname, std::multimap<uint64_t, uint64_t>& index_se) {
    struct stat info;
    if (   stat(dbname.c_str(), &info) != 0
        && db_options_.create_if_missing
        && mkdir(dbname.c_str(), 0644) < 0) {
      return Status::IOError("Could not create directory", strerror(errno));
    }
    
    if(!(info.st_mode & S_IFDIR)) {
      return Status::IOError("A file with same name as the database already exists and is not a directory. Remove or rename this file to continue.", dbname.c_str());
    }

    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }

    // Sort the fileids by <timestamp, fileid>, so that puts and removes can be
    // applied in the right order.
    // Indeed, imagine that we have files with ids from 1 to 100, and a
    // compaction process operating on files 1 through 50. The files 1-50 are
    // going to be compacted and the result of this compaction written
    // to ids 101 and above, which means that even though the entries in
    // files 101 and above are older than the entries in files 51-100, they are
    // in files with greater ids. Thus, the file ids cannot be used as a safe
    // way to order the entries in a set of files, and we need to have a sequence id
    // which will allow all other processes to know what is the order of
    // the entries in a set of files, which is why we have a 'timestamp' in each
    // file. As a consequence, the sequence id is the concatenation of
    // the 'timestamp' and the 'fileid'.
    // As the compaction process will always include at least one uncompacted
    // file, the maximum timestamp is garanteed to be always increasing and no
    // overlapping will occur.
    std::map<std::string, uint32_t> timestamp_fileid_to_fileid;
    char filepath[2048];
    char buffer_key[128];
    uint32_t fileid_max = 0;
    uint64_t timestamp_max = 0;
    uint32_t fileid = 0;
    Status s;
    while ((entry = readdir(directory)) != NULL) {
      sprintf(filepath, "%s/%s", dbname.c_str(), entry->d_name);
      if (strncmp(entry->d_name, "compaction", 10) == 0) continue;
      if (stat(filepath, &info) != 0 || !(info.st_mode & S_IFREG)) continue;
      fileid = LogfileManager::hex_to_num(entry->d_name);
      fprintf(stderr, "file: [%s] [%lld] [%u]\n", entry->d_name, info.st_size, fileid);
      if (info.st_size <= SIZE_LOGFILE_HEADER) {
        fprintf(stderr, "file: [%s] only has a header or less, skipping\n", entry->d_name);
        continue;
      }

      Mmap mmap(filepath, info.st_size);
      struct LogFileHeader* lfh = reinterpret_cast<struct LogFileHeader*>(mmap.datafile());
      // TODO: need CRC32 on the header

      //std::string key = std::to_string(lfh->timestamp) + "-" + std::to_string(fileid);
      sprintf(buffer_key, "%016llX-%016X", lfh->timestamp, fileid);
      std::string key(buffer_key);
      timestamp_fileid_to_fileid[key] = fileid;
      fileid_max = std::max(fileid_max, fileid);
      timestamp_max = std::max(timestamp_max, lfh->timestamp);
    }

    //while ((entry = readdir(directory)) != NULL) {
    for (auto& p: timestamp_fileid_to_fileid) {
      //sprintf(filepath, "%s/%s", dbname.c_str(), entry->d_name);
      //if (strncmp(entry->d_name, "compaction", 10) == 0) continue;
      //fileid = LogfileManager::hex_to_num(entry->d_name);
      uint32_t fileid = p.second;
      std::string filepath = GetFilepath(fileid);
      LOG_TRACE("LogfileManager::LoadDatabase()", "Loading file:[%s] with key:[%s]", filepath.c_str(), p.first.c_str());
      if (stat(filepath.c_str(), &info) != 0) continue;
      Mmap mmap(filepath.c_str(), info.st_size);
      s = LoadFile(mmap, fileid, index_se);
      if (!s.IsOK()) {
        LOG_WARN("LogfileManager::LoadDatabase()", "Could not load index in file [%s], entering recovery mode", filepath.c_str());
        s = RecoverFile(mmap, fileid, index_se);
      }
      if (!s.IsOK()) {
        LOG_WARN("LogfileManager::LoadDatabase()", "Recovery failed for file [%s]", filepath.c_str());
      }
    }
    SetSequenceFileId(fileid_max + 1);
    SetSequenceTimestamp(timestamp_max + 1);
    closedir(directory);
    return Status::OK();
  }

  Status LoadFile(Mmap& mmap,
                  uint32_t fileid,
                  std::multimap<uint64_t, uint64_t>& index_se) {
    // TODO-17: need to check CRC32 for the footer and the footer indexes.
    // TODO-15: handle large file (with very large, unique entry)

    struct LogFileFooter* footer = reinterpret_cast<struct LogFileFooter*>(mmap.datafile() + mmap.filesize() - sizeof(struct LogFileFooter));
    int rewind = sizeof(struct LogFileFooter) + footer->num_entries * (sizeof(struct LogFileFooterIndex));
    if (   footer->magic_number == get_magic_number()
        && rewind >= 0
        && rewind <= mmap.filesize() - SIZE_LOGFILE_HEADER) {
      // The file has a clean footer, load all the offsets in the index
      uint64_t offset_index = mmap.filesize() - rewind;
      for (auto i = 0; i < footer->num_entries; i++) {
        auto item = reinterpret_cast<struct LogFileFooterIndex*>(mmap.datafile() + offset_index);
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        index_se.insert(std::pair<uint64_t, uint64_t>(item->hashed_key, fileid_shifted | item->offset_entry));
        LOG_TRACE("LoadFile()", "Add item to index -- hashed_key:[%llu] offset:[%u] -- offset_index:[%llu] -- sizeof(struct):[%d]", item->hashed_key, item->offset_entry, offset_index, sizeof(struct LogFileFooterIndex));
        offset_index += sizeof(struct LogFileFooterIndex);
      }
      file_resource_manager.SetFileSize(fileid, mmap.filesize());
      if (footer->filetype == kLargeType) {
        file_resource_manager.SetFileLarge(fileid);
      }
      LOG_TRACE("LoadFile()", "Loaded [%s] num_entries:[%llu] rewind:[%llu]", mmap.filepath(), footer->num_entries, rewind);
    } else {
      // The footer is corrupted: go through every item and verify it
      LOG_TRACE("LoadFile()", "Skipping [%s] - magic_number:[%llu/%llu] rewind:[%d]", mmap.filepath(), footer->magic_number, get_magic_number(), rewind);
      return Status::IOError("Invalid footer");
    }

    //LOG_TRACE("LoadFile()", "Invalid magic number for file [%s]", filename);
    //return Status::IOError("Invalid magic number");
    // TODO-15: add the recovery code here
    return Status::OK();
  }

  Status RecoverFile(Mmap& mmap,
                     uint32_t fileid,
                     std::multimap<uint64_t, uint64_t>& index_se) {
    uint32_t offset = SIZE_LOGFILE_HEADER;
    std::vector< std::pair<uint64_t, uint32_t> > logindex_current;
    bool has_padding_in_values = false;
    bool has_invalid_entries   = false;

    struct LogFileHeader* lfh = reinterpret_cast<struct LogFileHeader*>(mmap.datafile());
    if (lfh->filetype == kLargeType) {
      // TODO: what to do with this file? remove it? quarantine it? Let the user
      // choose based on options?
      LOG_TRACE("Logmanager::RecoverFile", "Could not recover file [%s]\n", mmap.filepath());
      return Status::OK();
    }

    while (true) {
      struct Entry* entry = reinterpret_cast<struct Entry*>(mmap.datafile() + offset);
      if (   offset + sizeof(struct Entry) >= mmap.filesize()
          || entry->size_key == 0
          || offset + sizeof(struct Entry) + entry->size_key > mmap.filesize()
          || offset + sizeof(struct Entry) + entry->size_key + entry->size_value_offset() > mmap.filesize()) {
        // end of file
        LOG_TRACE("Logmanager::RecoverFile", "end of file [%llu] [%llu] [%llu] [%llu] - filesize:[%llu]", 
          (uint64_t)offset + sizeof(struct Entry),
          entry->size_key,
          (uint64_t)offset + sizeof(struct Entry) + entry->size_key,
          (uint64_t)offset + sizeof(struct Entry) + entry->size_key + entry->size_value_offset(),
          (uint64_t)mmap.filesize());
        break;
      }
      crc32_.ResetThreadLocalStorage();
      // TODO-17: need a way to check the crc32 for the entry header and the key
      //          maybe the CRC32 could be computed on the final frames, and not
      //          the data inside of the frames -- need to check if the CRC32
      //          values are the same in both cases though:
      //          We compress data and create frames with it. Are the CRC32 the
      //          sames if:
      //             1. it is computed over the sequence of frames,
      //             2. it is computed over each frame separately then added.
      crc32_.stream(mmap.datafile() + sizeof(struct Entry) + entry->size_key, entry->size_value_used());
      if (true || entry->crc32 == crc32_.get()) { // TODO-17: fix CRC32 check
        // Valid content, add to index
        // TODO-18: make sure invalid entries get marked as invalid so that the
        //          compaction process can clean them up
        logindex_current.push_back(std::pair<uint64_t, uint32_t>(entry->hash, offset));
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        index_se.insert(std::pair<uint64_t, uint64_t>(entry->hash, fileid_shifted | offset));
      } else {
        has_invalid_entries = true; 
      }
      if (entry->HasPadding()) has_padding_in_values = true;
      offset += sizeof(struct Entry) + entry->size_key + entry->size_value_offset();
      LOG_TRACE("Logmanager::RecoverFile", "Recovered hash [%llu], next offset [%llu]", entry->hash, offset);
    }

    if (offset > SIZE_LOGFILE_HEADER) {
      mmap.Close();
      int fd;
      if ((fd = open(mmap.filepath(), O_WRONLY, 0644)) < 0) {
        LOG_EMERG("Logmanager::RecoverFile()", "Could not open file [%s]: %s", mmap.filepath(), strerror(errno));
        return Status::IOError("Could not open file for recovery", mmap.filepath());
      }
      ftruncate(fd, offset);
      lseek(fd, 0, SEEK_END);
      uint64_t size_logindex;
      // NOTE: fine to use kLogType as kLargeType files will not be recovered anyway
      WriteLogIndex(fd, logindex_current, &size_logindex, kLogType, has_padding_in_values, has_invalid_entries);
      file_resource_manager.SetFileSize(fileid, mmap.filesize() + size_logindex);
      close(fd);
    } else {
      // TODO: were not able to recover anything in the file
      LOG_EMERG("Logmanager::RecoverFile()", "Was not able to recover anything from file [%s]", mmap.filepath());
    }

    return Status::OK();
  }

  uint64_t static get_magic_number() { return 0x4d454f57; }

 private:
  // Options
  DatabaseOptions db_options_;
  Hash *hash_;
  bool is_closed_;

  uint32_t fileid_;
  uint32_t sequence_fileid_;
  std::mutex mutex_sequence_fileid_;

  uint64_t timestamp_;
  uint64_t sequence_timestamp_;
  std::mutex mutex_sequence_timestamp_;
  bool is_locked_sequence_timestamp_;

  int size_block_;
  bool has_file_;
  int fd_;
  std::string filepath_;
  uint64_t offset_start_;
  uint64_t offset_end_;
  std::string dbname_;
  char *buffer_raw_;
  char *buffer_index_;
  bool buffer_has_items_;
  kdb::CRC32 crc32_;
  std::string prefix_;

 public:
  FileResourceManager file_resource_manager;

  // key_to_location is made to be dependent on the id of the thread that
  // originated an order, so that if two writers simultaneously write entries
  // with the same key, they will be properly stored into separate locations.
  // NOTE: if a thread crashes or terminates, its data will *not* be cleaned up.
  std::map< std::thread::id, std::map<std::string, uint64_t> > key_to_location;
};


class StorageEngine {
 public:
  StorageEngine(DatabaseOptions db_options, std::string dbname, int size_block=0)
      : db_options_(db_options),
        logfile_manager_(db_options, dbname, ""),
        logfile_manager_compaction_(db_options, dbname, "compaction_") {
    LOG_TRACE("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
    thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
    thread_compaction_ = std::thread(&StorageEngine::ProcessingLoopCompaction, this);
    num_readers_ = 0;
    is_compaction_in_progress_ = false;
    hash_ = MakeHash(db_options.hash);
    logfile_manager_.LoadDatabase(dbname, index_);
  }

  ~StorageEngine() {
    thread_index_.join();
    thread_data_.join();
    thread_compaction_.join();
  }

  void Close() {
    // Wait for readers to exit
    AcquireWriteLock();
    logfile_manager_.Close();
    ReleaseWriteLock();
    LOG_TRACE("StorageEngine::Close()", "done");
  }

  void ProcessingLoopCompaction() {
    std::chrono::milliseconds duration(10000);
    std::chrono::milliseconds forever(100000000000000000);
    while(true) {
      struct stat info;
      if (stat("/tmp/do_compaction", &info) == 0) {
        uint32_t seq = logfile_manager_.GetSequenceFileId();
        Compaction(dbname_, 1, seq+1); 
        std::this_thread::sleep_for(forever);
      }
      std::this_thread::sleep_for(duration);
    }
  }

  void ProcessingLoopData() {
    while(true) {
      // Wait for orders to process
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "start");
      //std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
      std::vector<Order> orders = EventManager::flush_buffer.Wait();
      //std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
      //uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      //std::cout << "buffer read from storage engine in " << duration << " ms" << std::endl;
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
  Status Get(ByteArray* key, ByteArray** value_out) {
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
    if (has_compaction_index) s = GetWithIndex(index_compaction_, key, value_out);
    if (!s.IsOK()) s = GetWithIndex(index_, key, value_out);

    mutex_read_.lock();
    num_readers_ -= 1;
    LOG_TRACE("GetEntry()", "num_readers_: %d", num_readers_);
    mutex_read_.unlock();
    cv_read_.notify_one();

    return s;
  }

  // NOTE: value_out must be deleled by the caller
  Status GetWithIndex(std::multimap<uint64_t, uint64_t>& index,
                      ByteArray* key,
                      ByteArray** value_out) {
    std::unique_lock<std::mutex> lock(mutex_index_);
    // TODO-26: should not be locking here, instead, should store the hashed key
    // and location from the index and release the lock right away -- should not
    // be locking while calling GetEntry()
    
    LOG_TRACE("StorageEngine::Get()", "%s", key->ToString().c_str());

    // NOTE: Since C++11, the relative ordering of elements with equivalent keys
    //       in a multimap is preserved.
    uint64_t hashed_key = hash_->HashFunction(key->data(), key->size());
    auto range = index.equal_range(hashed_key);
    auto rbegin = --range.second;
    auto rend  = --range.first;
    for (auto it = rbegin; it != rend; --it) {
      ByteArray *key_temp;
      Status s = GetEntry(it->second, &key_temp, value_out); 
      LOG_TRACE("StorageEngine::Get()", "key:[%s] key_temp:[%s] hashed_key:[%llu] hashed_key_temp:[%llu] size_key:[%llu] size_key_temp:[%llu]", key->ToString().c_str(), key_temp->ToString().c_str(), hashed_key, it->first, key->size(), key_temp->size());
      if (*key_temp == *key) {
        delete key_temp;
        if (s.IsRemoveOrder()) {
          s = Status::NotFound("Unable to find the entry in the storage engine (remove order)");
        }
        return s;
      }
      delete key_temp;
      delete *value_out;
    }
    LOG_TRACE("StorageEngine::Get()", "%s - not found!", key->ToString().c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }


  // NOTE: key_out and value_out must be deleted by the caller
  Status GetEntry(uint64_t location, ByteArray **key_out, ByteArray **value_out) {
    LOG_TRACE("StorageEngine::GetEntry()", "start");
    Status s = Status::OK();

    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    uint64_t filesize = 0;
    // NOTE: used to be in mutex_write_ and mutex_read_ -- if crashing, put the
    //       mutexes back
    filesize = logfile_manager_.file_resource_manager.GetFileSize(fileid);

    LOG_TRACE("StorageEngine::GetEntry()", "location:%llu fileid:%u offset_file:%u filesize:%llu", location, fileid, offset_file, filesize);
    std::string filepath = logfile_manager_.GetFilepath(fileid); // TODO: optimize here

    auto key_temp = new SharedMmappedByteArray(filepath,
                                               filesize);

    auto value_temp = new SharedMmappedByteArray();
    *value_temp = *key_temp;

    struct Entry* entry = reinterpret_cast<struct Entry*>(value_temp->datafile() + offset_file);
    key_temp->SetOffset(offset_file + sizeof(struct Entry), entry->size_key);
    value_temp->SetOffset(offset_file + sizeof(struct Entry) + entry->size_key, entry->size_value);
    value_temp->SetSizeCompressed(entry->size_value_compressed);
    value_temp->SetCRC32(entry->crc32);

    if (!entry->IsEntryFull()) {
      LOG_EMERG("StorageEngine::GetEntry()", "Entry is not of type FULL, which is not supported");
      s = Status::IOError("Entries of type not FULL are not supported");
    }

    if (entry->IsTypeRemove()) {
      s = Status::RemoveOrder();
      delete value_temp;
      value_temp = nullptr;
    }

    LOG_DEBUG("StorageEngine::GetEntry()", "mmap() out - type remove:%d", entry->IsTypeRemove());

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

    // TODO-23: replace the change on is_compaction_in_progress_ by a RAII
    //          WARNING: this is not the only part of the code with this issue,
    //          some code digging in all files is required
    mutex_compaction_.lock();
    is_compaction_in_progress_ = true;
    mutex_compaction_.unlock();

    // 1. Get the files needed for compaction
    // TODO: This is a quick hack to get the files for compaction, by going
    //       through all the files. Fix that to be only the latest non-handled
    //       log files
    LOG_TRACE("Compaction()", "Get files");
    std::multimap<uint64_t, uint64_t> index_compaction;
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }
    char filepath[2048];
    uint32_t fileid = 0;
    Status s;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      sprintf(filepath, "%s/%s", dbname.c_str(), entry->d_name);
      fileid = LogfileManager::hex_to_num(entry->d_name);
      if (   strncmp(entry->d_name, "compaction", 10) == 0
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
    LOG_TRACE("Compaction()", "Get unique hashed keys");
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
    LOG_TRACE("Compaction()", "Determine locations");
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
    LOG_TRACE("Compaction()", "Building clusters");
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
    LOG_TRACE("Compaction()", "Mmap() all the files! ALL THE FILES!");
    std::map<uint32_t, Mmap*> mmaps;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
      struct stat info;
      std::string filepath = logfile_manager_.GetFilepath(fileid);
      if (stat(filepath.c_str(), &info) != 0 || !(info.st_mode & S_IFREG)) {
        fprintf(stderr, "Error during compaction with file [%s]", filepath.c_str());
      }
      Mmap *mmap = new Mmap(filepath.c_str(), info.st_size);
      mmaps[fileid] = mmap;
    }


    // 6. Now building a vector of orders, that will be passed to the
    //    logmanager_compaction_ object to persist them on disk
    LOG_TRACE("Compaction()", "Build order list");
    std::vector<Order> orders;
    uint64_t timestamp_max = 0;
    for (auto it = fileids_compaction.begin(); it != fileids_compaction.end(); ++it) {
      uint32_t fileid = *it;
      if (IsFileLarge(fileid)) continue;
      Mmap* mmap = mmaps[fileid];

      // Check the footer
      struct LogFileFooter* footer = reinterpret_cast<struct LogFileFooter*>(mmap->datafile() + mmap->filesize() - sizeof(struct LogFileFooter));
      int rewind = sizeof(struct LogFileFooter) + footer->num_entries * (sizeof(struct LogFileFooterIndex));
      if (   footer->magic_number != LogfileManager::get_magic_number()
          || rewind < 0
          || rewind > mmap->filesize() - SIZE_LOGFILE_HEADER) {
        // TODO: handle error
        fprintf(stderr, "Compaction - invalid footer\n");
      }

      // Update the maximimum timestamp
      struct LogFileHeader* lfh = reinterpret_cast<struct LogFileHeader*>(mmap->datafile());
      timestamp_max = std::max(timestamp_max, lfh->timestamp);

      // Process entries in the file
      uint32_t offset = SIZE_LOGFILE_HEADER;
      uint64_t offset_end = mmap->filesize() - rewind;
      while (offset < offset_end) {
        LOG_TRACE("Compaction()", "order list loop - offset:%u offset_end:%u", offset, offset_end);
        struct Entry* entry = reinterpret_cast<struct Entry*>(mmap->datafile() + offset);
        if (   offset + sizeof(struct Entry) >= mmap->filesize()
            || entry->size_key == 0
            || offset + sizeof(struct Entry) + entry->size_key > mmap->filesize()
            || offset + sizeof(struct Entry) + entry->size_key + entry->size_value_offset() > mmap->filesize()) {
          // TODO: handle error
          fprintf(stderr, "Compaction - unexpected end of file - mmap->filesize():%d\n", mmap->filesize());
          entry->print();
          break;
        }

        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        uint64_t location = fileid_shifted | offset;

        LOG_TRACE("Compaction()", "order list loop - check if we should keep it - fileid:%u offset:%u", fileid, offset);
        if (   locations_delete.find(location) != locations_delete.end()
            || locations_secondary.find(location) != locations_secondary.end()) {
          offset += sizeof(struct Entry) + entry->size_key + entry->size_value_offset();
          continue;
        }

        // TODO-17: do CRC32 check
 
        // TODO-19: make function to get location from fileid and offset, and the
        //          fileid and offset from location
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
          struct Entry* entry = reinterpret_cast<struct Entry*>(mmap_location->datafile() + offset_file);

          LOG_TRACE("Compaction()", "order list loop - create byte arrays");
          ByteArray *key   = new SimpleByteArray(mmap_location->datafile() + offset_file + sizeof(struct Entry), entry->size_key);
          ByteArray *chunk = new SimpleByteArray(mmap_location->datafile() + offset_file + sizeof(struct Entry) + entry->size_key, entry->size_value_used());
          LOG_TRACE("Compaction()", "order list loop - push_back() orders");
          orders.push_back(Order{std::this_thread::get_id(),
                                 OrderType::Put,
                                 key,
                                 chunk,
                                 0,
                                 entry->size_value,
                                 entry->size_value_compressed,
                                 entry->crc32});
        }
        offset += sizeof(struct Entry) + entry->size_key + entry->size_value_offset();
      }
    }


    // 7. Write compacted orders on secondary storage
    LOG_TRACE("Compaction()", "Write compacted files");
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
    uint32_t num_files_compacted = logfile_manager_compaction_.GetSequenceFileId() - 1;
    uint32_t offset_fileid = logfile_manager_.IncrementSequenceFileId(num_files_compacted) - num_files_compacted;
    LOG_TRACE("Compaction()", "num_files_compacted:%u offset:%u", num_files_compacted, offset_fileid);


    // 9. Rename files
    for (auto fileid = 1; fileid <= num_files_compacted; fileid++) {
      LOG_TRACE("Compaction()", "Renaming [%s] into [%s]", logfile_manager_compaction_.GetFilepath(fileid).c_str(),
                                                           logfile_manager_.GetFilepath(fileid + offset_fileid).c_str());
      if (std::rename(logfile_manager_compaction_.GetFilepath(fileid).c_str(),
                      logfile_manager_.GetFilepath(fileid + offset_fileid).c_str()) != 0) {
        LOG_EMERG("Compaction()", "Could not rename file");
      }
    }

    
    // 10. Shift returned locations to match renamed files
    LOG_TRACE("Compaction()", "Shifting locations");
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
    //     stored in 'index_compactions_' into the main index 'index_'
    AcquireWriteLock();
    index_.insert(index_compaction_.begin(), index_compaction_.end()); 
    index_compaction_.clear();
    mutex_compaction_.lock();
    is_compaction_in_progress_ = false;
    mutex_compaction_.unlock();
    ReleaseWriteLock();


    // 14. Remove compacted files
    for (auto& fileid: fileids_compaction) {
      if (fileids_largefiles_keep.find(fileid) != fileids_largefiles_keep.end()) continue;
      LOG_TRACE("Compaction()", "Removing [%s]", logfile_manager_.GetFilepath(fileid).c_str());
      if (std::remove(logfile_manager_.GetFilepath(fileid).c_str()) != 0) {
        LOG_EMERG("Compaction()", "Could not remove file");
      }
    }

    // TODO-20: update changelogs and fsync() wherever necessary (journal, or whatever name, which has
    //          the sequence of operations that can be used to recover)
 
    return Status::OK();
  }


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

  // Compaction;
  LogfileManager logfile_manager_compaction_;
  std::mutex mutex_compaction_;
  bool is_compaction_in_progress_;
  std::thread thread_compaction_;
};

};

#endif // KINGDB_STORAGE_ENGINE_H_
