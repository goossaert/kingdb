// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "buffer_manager.h"

// TODO: add timer to force flush in case the buffer hasn't reached his maximum
//       capacity after N milliseconds

namespace kdb {

Status BufferManager::Get(const std::string& key, Value** value_out) {
  // TODO: need to fix the way the value is returned here: to create a new
  //       memory space and then return.
  // TODO: make sure the live buffer doesn't need to be protected by a mutex in
  //       order to be accessed -- right now I'm relying to timing, but that may
  //       be too weak to guarantee proper access

  // read the "live" buffer
  mutex_live_write_level1_.lock();
  mutex_indices_level3_.lock();
  auto& buffer_live = buffers_[im_live_];
  int num_items = buffer_live.size();
  mutex_indices_level3_.unlock();
  mutex_live_write_level1_.unlock();
  bool found = false;
  Order order_found;
  for (int i = 0; i < num_items; i++) {
    auto& order = buffer_live[i];
    if (order.key == key) {
      found = true;
      order_found = order;
    }
  }
  if (found) {
    if (   order_found.type == OrderType::Put
        && order_found.size_chunk == order_found.size_value) {
      *value_out = new ValueAllocated(order_found.chunk, order_found.size_chunk);
      return Status::OK();
    } else if (order_found.type == OrderType::Remove) {
      return Status::RemoveOrder("Unable to find entry");
    } else {
      return Status::NotFound("Unable to find entry");
    }
  }

  // prepare to read the "copy" buffer
  mutex_copy_write_level4_.lock();
  mutex_copy_read_level5_.lock();
  num_readers_ += 1;
  mutex_copy_read_level5_.unlock();
  mutex_copy_write_level4_.unlock();

  // read from "copy" buffer
  found = false;
  mutex_indices_level3_.lock();
  auto& buffer_copy = buffers_[im_copy_];
  mutex_indices_level3_.unlock();
  for (auto& order: buffer_copy) {
    if (order.key == key) {
      found = true;
      order_found = order;
    }
  }

  // exit the "copy" buffer
  mutex_copy_read_level5_.lock();
  num_readers_ -= 1;
  mutex_copy_read_level5_.unlock();
  cv_read_.notify_one();
  if (   found
      && order_found.type == OrderType::Put
      && order_found.size_chunk == order_found.size_value) {
    *value_out = new ValueAllocated(order_found.chunk, order_found.size_chunk);
    return Status::OK();
  } else if (   found
             && order_found.type == OrderType::Remove) {
    return Status::RemoveOrder("Unable to find entry");
  } else {
    return Status::NotFound("Unable to find entry");
  }
}


Status BufferManager::Put(const std::string& key, const std::string& value) {
  //return Write(OrderType::Put, key, value);
  return Status::InvalidArgument("BufferManager::Put() is not implemented");
}


Status BufferManager::PutChunk(const char* key,
                               uint64_t size_key,
                               const char* chunk,
                               uint64_t size_chunk,
                               uint64_t offset_chunk,
                               uint64_t size_value,
                               char * buffer_to_delete) {
  return WriteChunk(OrderType::Put,
                    key,
                    size_key,
                    chunk,
                    size_chunk,
                    offset_chunk,
                    size_value,
                    buffer_to_delete);
}




Status BufferManager::Remove(const std::string& key) {
  return WriteChunk(OrderType::Remove, key.c_str(), key.size(), nullptr, 0, 0, 0, nullptr);
}


Status BufferManager::WriteChunk(const OrderType& op,
                                 const char *key,
                                 uint64_t size_key,
                                 const char *chunk,
                                 uint64_t size_chunk,
                                 uint64_t offset_chunk,
                                 uint64_t size_value,
                                 char * buffer_to_delete) {
  std::unique_lock<std::mutex> lock_live(mutex_live_write_level1_);
  //if (key.size() + value.size() > buffer_size_) {
  //  return Status::InvalidArgument("Entry is too large.");
  //}
  LOG_TRACE("BufferManager", "Write() key:[%s] | size chunk:%d, total size value:%d offset_chunk:%llu sizeOfBuffer:%d", key, size_chunk, size_value, offset_chunk, buffers_[im_live_].size());

  // not sure if I should add the item then test, or test then add the item
  buffers_[im_live_].push_back(Order{op, key, size_key, chunk, size_chunk, offset_chunk, size_value, buffer_to_delete});
  if (offset_chunk == 0) {
    sizes_[im_live_] += size_key;
  }
  sizes_[im_live_] += size_chunk;

  /*
  if (buffers_[im_live_].size()) {
    for(auto &p: buffers_[im_live_]) {   
      LOG_TRACE("BufferManager", "Write() ITEM key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%llu sizeOfBuffer:%d sizes_[im_live_]:%d", p.key, p.key, p.size_chunk, p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
    }
  } else {
    LOG_TRACE("BufferManager", "Write() ITEM no buffers_[im_live_]");
  }
  */

  // NOTE: With multi-chunk entries, the last chunks may get stuck in the
  //       buffers without being flushed to secondary storage, and the storage
  //       engine will say that it doesn't has the item as the last chunk
  //       wasn't flushed yet.
  //       The use of 'force_swap_' here is a cheap bastard way of fixing the
  //       problem, by forcing the buffer to swap and flush for every last
  //       chunk encountered in a multi-chunk entry.
  //       If a Get() directly follows a Put() with a very low latency, this still
  //       won't fix the issue: needs a better solution on the long term.
  //       Builing the value by mixing data from the storage engine and the
  //       chunk in the buffers would be the best, but would add considerable
  //       complexity.
  //       => idea: return a "RETRY" command, indicating to the client that he
  //                needs to sleep for 100ms-ish and retry?
  if (   size_chunk + offset_chunk == size_value
      && offset_chunk > 0) {
    force_swap_ = true; 
  }

  if (sizes_[im_live_] > buffer_size_ || force_swap_) {
    std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
    if (can_swap_) {
      LOG_TRACE("BufferManager", "can_swap_ == true");
      std::unique_lock<std::mutex> lock_swap(mutex_indices_level3_);
      LOG_TRACE("BufferManager", "Swap buffers");
      can_swap_ = false;
      force_swap_ = false;
      std::swap(im_live_, im_copy_);
      cv_flush_.notify_one();
    } else {
      LOG_TRACE("BufferManager", "can_swap_ == false");
    }
  }
  return Status::OK();
}


void BufferManager::ProcessingLoop() {
  while(true) {
    LOG_TRACE("BufferManager", "ProcessingLoop() - start");
    std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
    if (sizes_[im_copy_] == 0) {
      LOG_TRACE("BufferManager", "ProcessingLoop() - wait");
      can_swap_ = true;
      cv_flush_.wait(lock_flush);
    }
  
    // Notify the storage engine that the buffer can be flushed
    LOG_TRACE("BM", "WAIT: Get()-flush_buffer");
    EventManager::flush_buffer.StartAndBlockUntilDone(buffers_[im_copy_]);

    // Wait for the index to notify the buffer manager
    LOG_TRACE("BM", "WAIT: Get()-clear_buffer");
    EventManager::clear_buffer.Wait();
    EventManager::clear_buffer.Done();
    
    // Wait for readers
    LOG_TRACE("BufferManager", "ProcessingLoop() - wait for lock 4");
    mutex_copy_write_level4_.lock();
    LOG_TRACE("BufferManager", "ProcessingLoop() - got lock 4");
    while(true) {
      std::unique_lock<std::mutex> lock_read(mutex_copy_read_level5_);
      if (num_readers_ == 0) break;
      LOG_TRACE("BufferManager", "ProcessingLoop() - wait for lock_read");
      cv_read_.wait(lock_read);
    }

    // Clear flush buffer
    sizes_[im_copy_] = 0;
    buffers_[im_copy_].clear();

    /*
    if (buffers_[im_copy_].size()) {
      for(auto &p: buffers_[im_copy_]) {
        LOG_TRACE("BufferManager", "ProcessingLoop() ITEM im_copy - key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%llu sizeOfBuffer:%d sizes_[im_copy_]:%d", p.key, p.key, p.size_chunk, p.size_value, p.offset_chunk, buffers_[im_copy_].size(), sizes_[im_copy_]);
      }
    } else {
      LOG_TRACE("BufferManager", "ProcessingLoop() ITEM no buffers_[im_copy_]");
    }

    if (buffers_[im_live_].size()) {
      for(auto &p: buffers_[im_live_]) {
        LOG_TRACE("BufferManager", "ProcessingLoop() ITEM im_live - key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%llu sizeOfBuffer:%d sizes_[im_live_]:%d", p.key, p.key, p.size_chunk, p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
      }
    } else {
      LOG_TRACE("BufferManager", "ProcessingLoop() ITEM no buffers_[im_live_]");
    }
    */

    can_swap_ = true;
    mutex_copy_write_level4_.unlock();
  }
}

};
