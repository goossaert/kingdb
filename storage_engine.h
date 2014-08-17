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



class LogfileManager {
 public:
  LogfileManager(DatabaseOptions& db_options, std::string dbname)
      : db_options_(db_options) {
    LOG_TRACE("LogfileManager::LogfileManager()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    sequence_fileid_ = 1;
    size_block_ = SIZE_LOGFILE_TOTAL;
    has_file_ = false;
    buffer_has_items_ = false;
    buffer_raw_ = new char[size_block_*2];
    buffer_index_ = new char[size_block_*2];
    hash_ = MakeHash(db_options.hash);
  }

  ~LogfileManager() {
    FlushCurrentFile();
    CloseCurrentFile();
    delete[] buffer_raw_;
    delete[] buffer_index_;
  }

  void SetFileSize(uint32_t fileid, uint64_t filesize) {
    file_sizes[fileid] = filesize; 
  }
  
  void SetSequenceFileId(uint32_t seq) {
    sequence_fileid_ = seq;
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
    //filepath_ = dbname_ + "/" + std::to_string(sequence_fileid_); // TODO: optimize here
    filepath_ = dbname_ + "/" + LogfileManager::num_to_hex(sequence_fileid_); // TODO: optimize here
    if ((fd_ = open(filepath_.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      LOG_EMERG("StorageEngine::ProcessingLoopData()", "Could not open file [%s]: %s", filepath_.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
    }
    has_file_ = true;
    // TODO: pre-shifting fileid_ here is weird -- either not shift, or change
    //       its name to make it clear that it's shifted
    fileid_ = sequence_fileid_;

    // Reserving space for header
    offset_start_ = 0;
    offset_end_ = SIZE_LOGFILE_HEADER;
    logindex_.clear();
  }

  void CloseCurrentFile() {
    close(fd_);
    sequence_fileid_ += 1;
    buffer_has_items_ = false;
    has_file_ = false;
  }

  void FlushCurrentFile(int force_new_file=0, uint64_t padding=0) {
    LOG_TRACE("LogfileManager::FlushCurrentFile()", "ENTER - fileid_:%d", fileid_);
    if (has_file_ && buffer_has_items_) {
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "has_files && buffer_has_items_ - fileid_:%d", fileid_);
      if (write(fd_, buffer_raw_ + offset_start_, offset_end_ - offset_start_) < 0) {
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "Error write(): %s", strerror(errno));
      }
      file_sizes[fileid_] = offset_end_;
      offset_start_ = offset_end_;
      buffer_has_items_ = false;
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "items written - offset_end_:%d | size_block_:%d | force_new_file:%d", offset_end_, size_block_, force_new_file);
    }

    if (padding) {
      offset_end_ += padding;
      offset_start_ = offset_end_;
      file_sizes[fileid_] = offset_end_;
      ftruncate(fd_, offset_end_);
      lseek(fd_, 0, SEEK_END);
    }

    if (offset_end_ >= size_block_ || (force_new_file && offset_end_ > SIZE_LOGFILE_HEADER)) {
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "file renewed - force_new_file:%d", force_new_file);
      uint64_t size_logindex;
      WriteLogIndex(fd_, logindex_, &size_logindex);
      offset_end_ += size_logindex;
      file_sizes[fileid_] = offset_end_;
      CloseCurrentFile();
      OpenNewFile();
    }
    LOG_TRACE("LogfileManager::FlushCurrentFile()", "done!");
  }

  Status FlushLogIndex() {
    uint64_t dummy_size_logindex;
    return WriteLogIndex(fd_, logindex_, &dummy_size_logindex);
  }


  Status WriteLogIndex(int fd,
                       std::vector< std::pair<uint64_t, uint32_t> >& logindex_current,
                       uint64_t* size_out) {
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
    footer->num_entries = logindex_current.size();
    footer->magic_number = get_magic_number();
    offset += sizeof(struct LogFileFooter);
    if (write(fd, buffer_index_, offset) < 0) {
      LOG_TRACE("StorageEngine::WriteLogIndex()", "Error write(): %s", strerror(errno));
    }
    *size_out = offset;
    fprintf(stderr, "WriteLogIndex() -- num_entries:[%lu]\n", logindex_current.size());
    return Status::OK();
  }


  uint64_t PrepareFileLargeOrder(Order& order, uint64_t hashed_key) {
    sequence_fileid_ += 1;
    uint64_t fileid_largefile = sequence_fileid_;
    std::string filepath = dbname_ + "/" + LogfileManager::num_to_hex(fileid_largefile); // TODO: optimize here
    LOG_TRACE("LogfileManager::PrepareFileLargeOrder()", "enter %s", filepath.c_str());
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      LOG_EMERG("StorageEngine::PrepareFileLargeOrder()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
    }

    char buffer[1024];
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer);
    entry->type = kPutEntry;
    entry->size_key = order.key->size();
    entry->size_value = order.size_value;
    entry->size_value_compressed = order.size_value_compressed;
    entry->hash = hashed_key;
    entry->crc32 = 0;
    if(write(fd, buffer_raw_, SIZE_LOGFILE_HEADER) < 0) { // write header
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, buffer, sizeof(struct Entry)) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, order.key->data(), order.key->size()) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, order.chunk->data(), order.chunk->size()) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }

    uint64_t filesize = SIZE_LOGFILE_HEADER + sizeof(struct Entry) + order.key->size() + order.size_value;
    ftruncate(fd, filesize);
    file_sizes[fileid_largefile] = filesize;
    close(fd);
    uint64_t fileid_shifted = fileid_largefile;
    fileid_shifted <<= 32;
    LOG_TRACE("LogfileManager::PrepareFileLargeOrder()", "fileid [%d]", fileid_largefile);
    return fileid_shifted | SIZE_LOGFILE_HEADER;
  }


  uint64_t WriteChunk(Order& order, uint64_t hashed_key, uint64_t location) {
    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    std::string filepath = dbname_ + "/" + LogfileManager::num_to_hex(fileid);
    LOG_TRACE("LogfileManager::WriteChunk()", "key [%s] filepath:[%s] offset_chunk:%llu", order.key->ToString().c_str(), filepath.c_str(), order.offset_chunk);
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY, 0644)) < 0) {
      LOG_EMERG("StorageEngine::WriteChunk()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
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
    //if (   order.size_value_compressed > 0
    //    && order.chunk->size() + order.offset_chunk == order.size_value_compressed) {
    if (   (order.size_value_compressed == 0 && order.chunk->size() + order.offset_chunk == order.size_value)
        || (order.size_value_compressed != 0 && order.chunk->size() + order.offset_chunk == order.size_value_compressed) ) {
      LOG_TRACE("LogfileManager::WriteChunk()", "Write compressed size: [%s] - size:%llu, compressed size:%llu crc32:%u", order.key->ToString().c_str(), order.size_value, order.size_value_compressed, order.crc32);
      struct Entry entry;
      entry.type = kPutEntry;
      entry.size_key = order.key->size();
      entry.size_value = order.size_value;
      entry.size_value_compressed = order.size_value_compressed;
      entry.hash = hashed_key;
      entry.crc32 = order.crc32;
      if (pwrite(fd, &entry, sizeof(struct Entry), offset_file) < 0) {
        LOG_TRACE("LogfileManager::WriteChunk()", "Error pwrite(): %s", strerror(errno));
      }
    }

    close(fd);
    LOG_TRACE("LogfileManager::WriteChunk()", "all good");
    return location;
  }


  uint64_t WriteSmallOrder(Order& order, uint64_t hashed_key) {
    uint64_t location_out = 0;
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer_raw_ + offset_end_);
    if (order.type == OrderType::Put) {
      entry->type = kPutEntry;
      entry->size_key = order.key->size();
      entry->size_value = order.size_value;
      entry->size_value_compressed = order.size_value_compressed;
      entry->hash = hashed_key;
      entry->crc32 = order.crc32;
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry), order.key->data(), order.key->size());
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry) + order.key->size(), order.chunk->data(), order.chunk->size());

      //map_index[order.key] = fileid_ | offset_end_;
      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      logindex_.push_back(std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      offset_end_ += sizeof(struct Entry) + order.key->size() + order.chunk->size();

      if (order.chunk->size() != order.size_value) {
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "BEFORE fileid_ %u", fileid_);
        FlushCurrentFile(0, order.size_value - order.chunk->size());
        // TODO: might be better to lseek() instead of doing a large write
        //offset_end_ += order.size_value - order.size_chunk;
        //FlushCurrentFile();
        //ftruncate(fd_, offset_end_);
        //lseek(fd_, 0, SEEK_END);
        LOG_TRACE("StorageEngine::ProcessingLoopData()", "AFTER fileid_ %u", fileid_);
      }
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Put [%s]", order.key->ToString().c_str());
    } else { // order.type == OrderType::Remove
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Remove [%s]", order.key->ToString().c_str());
      entry->type = kRemoveEntry;
      entry->size_key = order.key->size();
      entry->size_value = 0;
      entry->size_value_compressed = 0;
      entry->crc32 = 0;
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry), order.key->data(), order.key->size());

      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      logindex_.push_back(std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      //location_out = 0;
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
      // TODO: if the item is self-contained (unique chunk), then no need to
      //       have size_value space, size_value_compressed is enough.

      // NOTE: orders can be of various sizes: when using the storage engine as an
      // embedded engine, orders can be of any size, and when plugging the
      // storage engine to a network server, orders can be chucks of data.

      // 1. The order is the first chunk of a very large entry, so we
      //    create a very large file and write the first chunk in there
      uint64_t location = 0;
      if (   order.key->size() + order.size_value > size_block_ // TODO: shouldn't this be testing size_value_compressed as well?
          && order.offset_chunk == 0) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "1. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        location = PrepareFileLargeOrder(order, hashed_key);
      // 2. The order is a non-first chunk, so we
      //    open the file, pwrite() the chunk, and close the file.
      } else if (   order.offset_chunk != 0
                 /*
                 && (   (order.size_value_compressed == 0 && order.chunk->size() != order.size_value) // TODO: are those two tests on the size necessary?
                     || (order.size_value_compressed != 0 && order.chunk->size() != order.size_value_compressed)
                    )
                 */
                ) {
        //  TODO: replace the tests on compression "order.size_value_compressed ..." by a real test on a flag or a boolean
        //  TODO: replace the use of size_value or size_value_compressed by a unique size() which would already return the right value
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "2. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        location = key_to_location[order.key->ToString()];
        if (location != 0) {
          WriteChunk(order, hashed_key, location);
        } else {
          LOG_EMERG("StorageEngine", "Avoided catastrophic location error"); 
        }

      // 3. The order is the first chunk of a small or self-contained entry
      } else {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "3. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk);
        buffer_has_items_ = true;
        location = WriteSmallOrder(order, hashed_key);
      }

      // If the order was the self-contained or the last chunk, add his location to the output map_index_out[]
      if (   (order.size_value_compressed == 0 && order.offset_chunk + order.chunk->size() == order.size_value)
          || (order.size_value_compressed != 0 && order.offset_chunk + order.chunk->size() == order.size_value_compressed)) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "END OF ORDER key: [%s] size_chunk:%llu offset_chunk: %llu location:%llu", order.key->ToString().c_str(), order.chunk->size(), order.offset_chunk, location);
        if (location != 0) {
          map_index_out.insert(std::pair<uint64_t, uint64_t>(hashed_key, location));
        } else {
          LOG_EMERG("StorageEngine", "Avoided catastrophic location error"); 
        }
        key_to_location.erase(order.key->ToString());
      // Else, if the order is not self-contained and is the first chunk,
      // the location is saved in key_to_location[]
      } else if (order.offset_chunk == 0) {
        if (location != 0 && order.type != OrderType::Remove) {
          key_to_location[order.key->ToString()] = location;
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

    char filepath[2048];
    uint32_t fileid = 0;
    Status s;
    while ((entry = readdir(directory)) != NULL) {
      sprintf(filepath, "%s/%s", dbname.c_str(), entry->d_name);
      if (stat(filepath, &info) != 0 || !(info.st_mode & S_IFREG)) continue;
      fileid = LogfileManager::hex_to_num(entry->d_name);
      fprintf(stderr, "file: [%s] [%lld] [%u]\n", entry->d_name, info.st_size, fileid);
      if (info.st_size <= SIZE_LOGFILE_HEADER) {
        fprintf(stderr, "file: [%s] only has a header or less, skipping\n", entry->d_name);
        continue;
      }
      Mmap mmap(filepath, info.st_size);
      s = LoadFile(mmap, fileid, index_se);
      if (!s.IsOK()) {
        LOG_WARN("LogfileManager::LoadDatabase()", "Could not load index in file [%s], entering recovery mode", filepath);
        s = RecoverFile(mmap, fileid, index_se);
      }
      if (!s.IsOK()) {
        LOG_WARN("LogfileManager::LoadDatabase()", "Recovery failed for file [%s]", filepath);
      }
    }
    SetSequenceFileId(fileid + 1);
    closedir(directory);
    return Status::OK();
  }

  Status LoadFile(Mmap& mmap,
                  uint32_t fileid,
                  std::multimap<uint64_t, uint64_t>& index_se) {
    // TODO: need to check CRC32 for the footer and the footer indexes.
    // TODO: handle large file (with very large, unique entry)
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
      SetFileSize(fileid, mmap.filesize());
      LOG_TRACE("LoadFile()", "Loaded [%s] num_entries:[%llu] rewind:[%llu]", mmap.filepath(), footer->num_entries, rewind);
    } else {
      // The footer is corrupted: go through every item and verify it
      LOG_TRACE("LoadFile()", "Skipping [%s] - magic_number:[%llu/%llu] rewind:[%d]", mmap.filepath(), footer->magic_number, get_magic_number(), rewind);
      return Status::IOError("Invalid footer");
    }

    //LOG_TRACE("LoadFile()", "Invalid magic number for file [%s]", filename);
    //return Status::IOError("Invalid magic number");
    // TODO: add the recovery code here
    return Status::OK();
  }

  Status RecoverFile(Mmap& mmap,
                     uint32_t fileid,
                     std::multimap<uint64_t, uint64_t>& index_se) {
    uint32_t offset = SIZE_LOGFILE_HEADER;
    std::vector< std::pair<uint64_t, uint32_t> > logindex_current;
    while (true) {
      struct Entry* entry = reinterpret_cast<struct Entry*>(mmap.datafile() + offset);
      if (   offset + sizeof(struct Entry) >= mmap.filesize()
          || entry->size_key == 0
          || offset + sizeof(struct Entry) + entry->size_key > mmap.filesize()
          || offset + sizeof(struct Entry) + entry->size_key + entry->size_value > mmap.filesize()) {
        // end of file
        LOG_TRACE("Logmanager::RecoverFile", "end of file [%llu] [%llu] [%llu] - filesize:[%llu]", 
          (uint64_t)offset + sizeof(struct Entry),
          (uint64_t)SIZE_LOGFILE_HEADER + offset + entry->size_key,
          (uint64_t)SIZE_LOGFILE_HEADER + offset + entry->size_key + entry->size_value,
          (uint64_t)mmap.filesize());
        break;
      }
      crc32_.reset();
      // TODO: need a way to check the crc32 for the entry header and the key
      if (entry->size_value_compressed == 0) { // TODO: change for better way to detect compression
        crc32_.stream(mmap.datafile() + sizeof(struct Entry) + entry->size_key, entry->size_value);
      } else {
        crc32_.stream(mmap.datafile() + sizeof(struct Entry) + entry->size_key, entry->size_value_compressed);
      }
      if (true || entry->crc32 == crc32_.get()) { // TODO: fix CRC32 check
        // Valid content, add to index
        // TODO: make sure invalid entries get marked as invalid so that the
        //       compaction process can clean them up
        logindex_current.push_back(std::pair<uint64_t, uint32_t>(entry->hash, offset));
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        index_se.insert(std::pair<uint64_t, uint64_t>(entry->hash, fileid_shifted | offset));
      }
      offset += sizeof(struct Entry) + entry->size_key + entry->size_value;
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
      WriteLogIndex(fd, logindex_current, &size_logindex);
      file_sizes[fileid] = mmap.filesize() + size_logindex;
      close(fd);
    }

    return Status::OK();
  }

  uint64_t static get_magic_number() { return 0x4D454F57; }

 private:
  // Options
  DatabaseOptions db_options_;
  Hash *hash_;

  int sequence_fileid_;
  int size_block_;
  bool has_file_;
  int fd_;
  std::string filepath_;
  uint32_t fileid_;
  uint64_t offset_start_;
  uint64_t offset_end_;
  std::string dbname_;
  char *buffer_raw_;
  char *buffer_index_;
  bool buffer_has_items_;
  std::vector< std::pair<uint64_t, uint32_t> > logindex_;
  kdb::CRC32 crc32_;

 public:
  // TODO: make accessors for file_sizes that are protected by a mutex
  std::map<uint32_t, uint64_t> file_sizes; // fileid to file size
  std::map<std::string, uint64_t> key_to_location;
  // TODO: make sure that the case where two writers simultaneously write entries with the same key is taken 
  //       into account -- add thread id in the key of key_to_location?
  // TODO: make sure that the writes that fail gets all their temporary data
  //       cleaned up (including whatever is in key_to_location)
};


class StorageEngine {
 public:
  StorageEngine(DatabaseOptions db_options, std::string dbname, int size_block=0)
      : db_options_(db_options),
        logfile_manager_(db_options, dbname) {
    LOG_TRACE("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
    thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
    num_readers_ = 0;
    hash_ = MakeHash(db_options.hash);
    logfile_manager_.LoadDatabase(dbname, index_);
  }

  ~StorageEngine() {
    thread_index_.join();
    thread_data_.join();
  }

  void Close() {
    // Wait for readers to exit
    mutex_write_.lock();
    while(true) {
      std::unique_lock<std::mutex> lock_read(mutex_read_);
      if (num_readers_ == 0) break;
      cv_read_.wait(lock_read);
    }
    logfile_manager_.FlushLogIndex();
    logfile_manager_.CloseCurrentFile();
    fprintf(stderr, "Close file\n");
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

      // Wait for readers to exit
      mutex_write_.lock();
      while(true) {
        std::unique_lock<std::mutex> lock_read(mutex_read_);
        if (num_readers_ == 0) break;
        cv_read_.wait(lock_read);
      }

      // Process orders, and create update map for the index
      std::multimap<uint64_t, uint64_t> map_index;
      logfile_manager_.WriteOrdersAndFlushFile(orders, map_index);
      
      // Release lock and handle events
      mutex_write_.unlock();

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

      for (auto& p: index_updates) {
        //uint64_t hashed_key = hash_->HashFunction(p.first.c_str(), p.first.size());
        LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "hash [%llu] location [%llu]", p.first, p.second);
        index_.insert(std::pair<uint64_t,uint64_t>(p.first, p.second));
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

  // NOTE: value_out must be deleled by the caller
  Status Get(ByteArray* key, ByteArray** value_out) {
    //LOG_TRACE("INDEX", "WAIT: Get()-mutex_index_");
    std::unique_lock<std::mutex> lock(mutex_index_);
    LOG_TRACE("StorageEngine::Get()", "%s", key->ToString().c_str());

    // NOTE: Since C++11, the relative ordering of elements with equivalent keys
    //       in a multimap is preserved.
    uint64_t hashed_key = hash_->HashFunction(key->data(), key->size());
    auto range = index_.equal_range(hashed_key);
    auto rbegin = --range.second;
    auto rend  = --range.first;
    for (auto it = rbegin; it != rend; --it) {
      ByteArray *key_temp;
      Status s = GetEntry(it->second, &key_temp, value_out); 
      LOG_TRACE("StorageEngine::Get()", "key:[%s] key_temp:[%s] hashed_key:[%llu] hashed_key_temp:[%llu] size_key:[%llu] size_key_temp:[%llu]", key->ToString().c_str(), key_temp->ToString().c_str(), hashed_key, it->first, key->size(), key_temp->size());
      if (*key_temp == *key) {
        delete key_temp;
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
    mutex_write_.lock();
    mutex_read_.lock();
    num_readers_ += 1;
    filesize = logfile_manager_.file_sizes[fileid]; // TODO: check if file is in map
    mutex_read_.unlock();
    mutex_write_.unlock();

    LOG_TRACE("StorageEngine::GetEntry()", "location:%llu fileid:%u offset_file:%u filesize:%llu", location, fileid, offset_file, filesize);
    std::string filepath = dbname_ + "/" + LogfileManager::num_to_hex(fileid); // TODO: optimize here

    auto key_temp = new SharedMmappedByteArray(filepath,
                                               filesize);

    auto value_temp = new SharedMmappedByteArray();
    *value_temp = *key_temp;

    struct Entry* entry = reinterpret_cast<struct Entry*>(value_temp->datafile() + offset_file);
    key_temp->SetOffset(offset_file + sizeof(struct Entry), entry->size_key);
    value_temp->SetOffset(offset_file + sizeof(struct Entry) + entry->size_key, entry->size_value);
    value_temp->SetSizeCompressed(entry->size_value_compressed);
    value_temp->SetCRC32(entry->crc32);

    if (entry->type == kRemoveEntry) {
      s = Status::NotFound("Unable to find the entry in the storage engine");
      delete value_temp;
      value_temp = nullptr;
    }

    LOG_DEBUG("StorageEngine::GetEntry()", "mmap() out - type:%d", entry->type);

    mutex_read_.lock();
    num_readers_ -= 1;
    LOG_TRACE("GetEntry()", "num_readers_: %d", num_readers_);
    mutex_read_.unlock();
    cv_read_.notify_one();

    *key_out = key_temp;
    *value_out = value_temp;
    return s;
  }

 private:
  // Options
  DatabaseOptions db_options_;

  // Data
  std::string dbname_;
  std::map<uint64_t, std::string> data_;
  std::map<std::string, uint64_t> key_to_location_;
  std::thread thread_data_;
  std::condition_variable cv_read_;
  std::mutex mutex_read_;
  std::mutex mutex_write_;
  int num_readers_;

  // Index
  std::multimap<uint64_t, uint64_t> index_;
  std::thread thread_index_;
  std::mutex mutex_index_;

  Hash *hash_;
  LogfileManager logfile_manager_;
};

};

#endif // KINGDB_STORAGE_ENGINE_H_
