// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed the BSD 3-Clause License,
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

#include "kdb.h"
#include "common.h"

namespace kdb {


class LogfileManager {
 public:
  LogfileManager(std::string dbname) {
    LOG_TRACE("LogfileManager::LogfileManager()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    sequence_fileid_ = 1;
    size_block_ = SIZE_LOGFILE_TOTAL;
    has_file_ = false;
    buffer_has_items_ = false;
    buffer_raw_ = new char[size_block_*2];
  }

  ~LogfileManager() {
    FlushCurrentFile();
    CloseCurrentFile();
    delete[] buffer_raw_;
  }

  void OpenNewFile() {
    filepath_ = dbname_ + "/" + std::to_string(sequence_fileid_); // TODO: optimize here
    if ((fd_ = open(filepath_.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      std::string msg = std::string("Count not open file [") + filepath_ + std::string("]");
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "%s: %s", msg.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
    }
    has_file_ = true;
    // TODO: pre-shifting fileid_ here is weird -- either not shift, or change
    //       its name to make it clear that it's shifted
    fileid_ = sequence_fileid_;

    // Reserving space for header
    offset_start_ = 0;
    offset_end_ = SIZE_LOGFILE_HEADER;
  }

  void CloseCurrentFile() {
    close(fd_);
    sequence_fileid_ += 1;
    buffer_has_items_ = false;
    has_file_ = false;
  }

  void FlushCurrentFile(int force_new_file=0) {
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

    if (offset_end_ >= size_block_ || (force_new_file && offset_end_ > SIZE_LOGFILE_HEADER)) {
      CloseCurrentFile();
      OpenNewFile();
      LOG_TRACE("LogfileManager::FlushCurrentFile()", "file renewed - force_new_file:%d", force_new_file);
    }
    LOG_TRACE("LogfileManager::FlushCurrentFile()", "done!");
  }


  uint64_t PrepareFileLargeOrder(Order& order) {
    sequence_fileid_ += 1;
    uint64_t fileid_largefile = sequence_fileid_;
    std::string filepath = dbname_ + "/" + std::to_string(fileid_largefile); // TODO: optimize here
    LOG_TRACE("LogfileManager::PrepareFileLargeOrder()", "enter %s", filepath.c_str());
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      std::string msg = std::string("Count not open file [") + filepath + std::string("]");
      LOG_TRACE("StorageEngine::PrepareFileLargeOrder()", "%s: %s", msg.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
    }

    char buffer[1024];
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer);
    entry->action_type = 7;
    entry->size_key = order.size_key;
    entry->size_value = order.size_value;
    entry->hash = 0;
    if(write(fd, buffer_raw_, SIZE_LOGFILE_HEADER) < 0) { // write header
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, buffer, sizeof(struct Entry)) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, order.key, order.size_key) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }
    if(write(fd, order.chunk, order.size_chunk) < 0) {
      LOG_TRACE("LogfileManager::FlushLargeOrder()", "Error write(): %s", strerror(errno));
    }

    uint64_t filesize = SIZE_LOGFILE_HEADER + sizeof(struct Entry) + order.size_key + order.size_value;
    ftruncate(fd, filesize);
    file_sizes[fileid_largefile] = filesize;
    close(fd);
    uint64_t fileid_shifted = fileid_largefile;
    fileid_shifted <<= 32;
    LOG_TRACE("LogfileManager::PrepareFileLargeOrder()", "fileid [%d]", fileid_largefile);
    return fileid_shifted | SIZE_LOGFILE_HEADER;
  }


  uint64_t WriteChunk(Order& order, uint64_t location) {
    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    std::string filepath = dbname_ + "/" + std::to_string(fileid);
    LOG_TRACE("LogfileManager::WriteChunk()", "key [%s] filepath:[%s] offset_chunk:%llu", order.key, filepath.c_str(), order.offset_chunk);
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY, 0644)) < 0) {
      std::string msg = std::string("Count not open file [") + filepath + std::string("]");
      LOG_TRACE("StorageEngine::WriteChunk()", "%s: %s", msg.c_str(), strerror(errno));
      exit(-1); // TODO: gracefully open() errors
    }

    if (pwrite(fd,
               order.chunk,
               order.size_chunk,
               offset_file + sizeof(struct Entry) + order.size_key + order.offset_chunk) < 0) {
      LOG_TRACE("LogfileManager::WriteChunk()", "Error pwrite(): %s", strerror(errno));
    }

    close(fd);
    LOG_TRACE("LogfileManager::WriteChunk()", "all good");
    return location;
  }


  uint64_t WriteSmallOrder(Order& order) {
    uint64_t offset_out = 0;
    struct Entry* entry = reinterpret_cast<struct Entry*>(buffer_raw_ + offset_end_);
    if (order.type == OrderType::Put) {
      entry->action_type = 7;
      entry->size_key = order.size_key;
      entry->size_value = order.size_value;
      entry->hash = 0;
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry), order.key, order.size_key);
      memcpy(buffer_raw_ + offset_end_ + sizeof(struct Entry) + order.size_key, order.chunk, order.size_chunk);
      //map_index[order.key] = fileid_ | offset_end_;
      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      offset_out = fileid_shifted | offset_end_;
      offset_end_ += sizeof(struct Entry) + order.size_key + order.size_chunk;

      if (order.size_chunk != order.size_value) {
        offset_end_ += order.size_value - order.size_chunk;
        FlushCurrentFile();
        ftruncate(fd_, offset_end_);
        lseek(fd_, 0, SEEK_END);
      }
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Put [%s]", order.key);
    } else { // order.type == OrderType::Remove
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "Remove [%s]", order.key);
      entry->action_type = 2;
      entry->size_key = order.size_key;
      entry->size_value = 0;
      memcpy(buffer_raw_ + sizeof(struct Entry), order.key, order.size_key);
      offset_end_ += sizeof(struct Entry) + order.size_key;
      offset_out = 0;
      //map_index[order.key] = 0;
    }
    return offset_out;
  }



  void WriteOrdersAndFlushFile(std::vector<Order>& orders, std::map<std::string, uint64_t>& map_index_out) {
    for (auto& order: orders) {

      if (!has_file_) OpenNewFile();

      if (offset_end_ > size_block_) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "About to flush - offset_end_: %llu | size_key: %d | size_value: %d | size_block_: %llu", offset_end_, order.size_key, order.size_value, size_block_);
        FlushCurrentFile(true);
      }

      // NOTE: orders can be of various sizes: when using the storage engine as an
      // embedded engine, orders can be of any size, and when plugging the
      // storage engine to a network server, orders can be chucks of data.

      // 1. The order is the first chunk of a very large entry, so we
      //    create a very large file and write the first chunk in there
      uint64_t location;
      if (   order.size_key + order.size_value > size_block_
          && order.offset_chunk == 0) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "1. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key, order.size_chunk, order.offset_chunk);
        location = PrepareFileLargeOrder(order);
      // 2. The order is a non-first chunk, so we
      //    open the file, pwrite() the chunk, and close the file.
      } else if (   order.size_chunk != order.size_value
                 && order.offset_chunk != 0) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "2. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key, order.size_chunk, order.offset_chunk);
        WriteChunk(order, key_to_location[order.key]);
        location = key_to_location[order.key];
      // 3. The order is the first chunk of a small or self-contained entry
      } else {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "3. key: [%s] size_chunk:%llu offset_chunk: %llu", order.key, order.size_chunk, order.offset_chunk);
        buffer_has_items_ = true;
        location = WriteSmallOrder(order);
      }

      // If the order was the self-contained or the last chunk, add his location
      // to the output map_index_out[]
      if (order.offset_chunk + order.size_chunk == order.size_value) {
        LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "END OF ORDER key: [%s] size_chunk:%llu offset_chunk: %llu location:%llu", order.key, order.size_chunk, order.offset_chunk, location);
        map_index_out[order.key] = location;
        key_to_location.erase(order.key);
        if (order.buffer_to_delete != nullptr) {
          delete[] order.key;
          delete[] order.buffer_to_delete;
        }
      // Else, if the order is not self-contained and is the fisrt chunk,
      // the location is saved in key_to_location[]
      } else if (order.offset_chunk == 0) {
        key_to_location[order.key] = location;
      }

    }
    LOG_TRACE("StorageEngine::WriteOrdersAndFlushFile()", "end flush");
    FlushCurrentFile();
  }

 private:
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
  bool buffer_has_items_;

 public:
  // TODO: make accessors for file_sizes that are protected by a mutex
  std::map<uint32_t, uint64_t> file_sizes; // fileid to file size
  std::map<std::string, uint64_t> key_to_location;
};


class StorageEngine {
 public:
  StorageEngine(std::string dbname, int size_block=0)
    : logfile_manager_(dbname) {
    LOG_TRACE("StorageEngine:StorageEngine()", "dbname: %s", dbname.c_str());
    dbname_ = dbname;
    thread_index_ = std::thread(&StorageEngine::ProcessingLoopIndex, this);
    thread_data_ = std::thread(&StorageEngine::ProcessingLoopData, this);
    num_readers_ = 0;
  }

  ~StorageEngine() {
    thread_index_.join();
    thread_data_.join();
  }

  void ProcessingLoopData() {
    while(true) {
   
      // Wait for orders to process
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "start");
      std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
      std::vector<Order> orders = EventManager::flush_buffer.Wait();     
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
      uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      std::cout << "buffer read from storage engine in " << duration << " ms" << std::endl;
      LOG_TRACE("StorageEngine::ProcessingLoopData()", "got %d orders", orders.size());

      // Wait for readers to exit
      mutex_write_.lock();
      while(true) {
        std::unique_lock<std::mutex> lock_read(mutex_read_);
        if (num_readers_ == 0) break;
        cv_read_.wait(lock_read);
      }

      // Process orders, and create update map for the index
      std::map<std::string, uint64_t> map_index;
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
      std::map<std::string, uint64_t> index_updates = EventManager::update_index.Wait();     
      LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "got index_updates");
      //LOG_TRACE("INDEX", "WAIT: loop:mutex_index_");
      mutex_index_.lock();
      for (auto& p: index_updates) {
        if (p.second == 0) {
          LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "erase [%s]", p.first.c_str());
          index_.erase(p.first);
        } else {
          LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "add [%s]", p.first.c_str());
          index_[p.first] = p.second; 
        }
      }
      mutex_index_.unlock();
      EventManager::update_index.Done();
      LOG_TRACE("StorageEngine::ProcessingLoopIndex()", "done");
      int temp = 1;
      //LOG_TRACE("INDEX", "WAIT: loop:clear_buffer");
      EventManager::clear_buffer.StartAndBlockUntilDone(temp);
    }
  }

  // NOTE: value_out must be deleled by the caller
  Status Get(const std::string& key, Value **value_out) {
    //LOG_TRACE("INDEX", "WAIT: Get()-mutex_index_");
    std::unique_lock<std::mutex> lock(mutex_index_);
    LOG_TRACE("StorageEngine::Get()", "%s", key.c_str());
    auto p = index_.find(key);
    if (p != index_.end()) {
      std::string *key_temp;
      Status s = GetEntry(index_[key], &key_temp, value_out); 
      LOG_TRACE("StorageEngine::Get()", "key:[%s] key_temp:[%s]", key.c_str(), key_temp->c_str());
      delete key_temp;
      return s;
      //return Status::OK();
    }
    LOG_TRACE("StorageEngine::Get()", "%s - not found!", key.c_str());
    return Status::NotFound("Unable to find the entry in the storage engine");
  }



  // NOTE: key_out and value_out must be deleted by the caller
  Status GetEntry(uint64_t offset, std::string **key_out, Value **value_out) {
    LOG_TRACE("StorageEngine::GetEntry()", "start");
    Status s = Status::OK();

    uint32_t fileid = (offset & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = offset & 0x00000000FFFFFFFF;
    uint64_t filesize = 0;
    mutex_write_.lock();
    mutex_read_.lock();
    num_readers_ += 1;
    filesize = logfile_manager_.file_sizes[fileid]; // TODO: check if file is in map
    mutex_read_.unlock();
    mutex_write_.unlock();

    LOG_TRACE("StorageEngine::GetEntry()", "fileid:%u offset_file:%u filesize:%llu", fileid, offset_file, filesize);
    std::string filepath = dbname_ + "/" + std::to_string(fileid); // TODO: optimize here

    *key_out   = new std::string("key-disabled");
    *value_out = new ValueMmap(filepath,
                               filesize,
                               offset_file);
    LOG_TRACE("StorageEngine::GetEntry()", "std::string: success");

    mutex_read_.lock();
    num_readers_ -= 1;
    LOG_TRACE("GetEntry()", "num_readers_: %d", num_readers_);
    mutex_read_.unlock();
    cv_read_.notify_one();
    return s;
  }

 private:
  // Data
  std::string dbname_;
  std::map<uint64_t, std::string> data_;
  std::map<std::string, uint64_t> key_to_offset_;
  std::thread thread_data_;
  std::condition_variable cv_read_;
  std::mutex mutex_read_;
  std::mutex mutex_write_;
  int num_readers_;

  // Index
  std::map<std::string, uint64_t> index_;
  std::thread thread_index_;
  std::mutex mutex_index_;

  LogfileManager logfile_manager_;
};

};

#endif // KINGDB_STORAGE_ENGINE_H_
