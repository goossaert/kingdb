// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "cache/write_buffer.h"

namespace kdb {

void WriteBuffer::Flush() {
  std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
  if (IsStopRequestedAndBufferEmpty()) return;
  // NOTE: Doing the flushing and waiting twice, in case the two buffers,
  // 'live' and 'copy', have items. This is a quick hack and a better
  // solution should be investigated.
  for (auto i = 0; i < 2; i++) {
    log::debug("LOCK", "2 lock");
    cv_flush_.notify_one();
    cv_flush_done_.wait_for(lock_flush, std::chrono::milliseconds(db_options_.write_buffer__close_timeout));
  }
  log::trace("WriteBuffer::Flush()", "end");
}

Status WriteBuffer::Get(ReadOptions& read_options, Kitten& key, Kitten* value_out) {
  // TODO: need to fix the way the value is returned here: to create a new
  //       memory space and then return.
  // TODO: make sure the live buffer doesn't need to be protected by a mutex in
  //       order to be accessed -- right now I'm relying on timing, but that may
  //       be too weak to guarantee proper access
  // TODO: for items being stored that are not small enough, only chunks will
  //       be found in the buffers -- should the kv-store return "not found"
  //       or should it try to send the data from the disk and the partially
  //       available chunks in the buffer?
  if (IsStopRequested()) return Status::IOError("Cannot handle request: WriteBuffer is closing");

  // read the "live" buffer
  mutex_live_write_level1_.lock();
  log::debug("LOCK", "1 lock");
  mutex_indices_level3_.lock();
  log::debug("LOCK", "3 lock");
  auto& buffer_live = buffers_[im_live_];
  int num_items = buffer_live.size();
  mutex_indices_level3_.unlock();
  log::debug("LOCK", "3 unlock");
  mutex_live_write_level1_.unlock();
  log::debug("LOCK", "1 unlock");
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
    log::debug("WriteBuffer::Get()", "found in buffer_live");
    if (   order_found.type == OrderType::Put
        && order_found.IsSelfContained()) {
      // TODO: uncompressed the chunk
      // TODO: make sure that it is clear for the code below this method
      // whether or not the chunk is compressed
      *value_out = order_found.chunk;
      (*value_out).set_size(order_found.size_value);
      (*value_out).set_size_compressed(order_found.size_value_compressed);
      return Status::OK();
    } else if (order_found.type == OrderType::Delete) {
      return Status::DeleteOrder();
    } else {
      return Status::NotFound("Unable to find entry");
    }
  }

  // prepare to read the "copy" buffer
  log::debug("LOCK", "4 lock");
  mutex_copy_write_level4_.lock();
  log::debug("LOCK", "5 lock");
  mutex_copy_read_level5_.lock();
  num_readers_ += 1;
  mutex_copy_read_level5_.unlock();
  log::debug("LOCK", "5 unlock");
  mutex_copy_write_level4_.unlock();
  log::debug("LOCK", "4 unlock");

  // read from "copy" buffer
  found = false;
  log::debug("LOCK", "3 lock");
  mutex_indices_level3_.lock();
  auto& buffer_copy = buffers_[im_copy_];
  mutex_indices_level3_.unlock();
  log::debug("LOCK", "3 unlock");
  for (auto& order: buffer_copy) {
    if (order.key == key) {
      found = true;
      order_found = order;
    }
  }

  Status s;
  if (found) log::debug("WriteBuffer::Get()", "found in buffer_copy");
  if (   found
      && order_found.type == OrderType::Put
      && order_found.IsSelfContained()) {
    // TODO: uncompressed the chunk
    // TODO: make sure that it is clear for the code below this method
    // whether or not the chunk is compressed
    *value_out = order_found.chunk;
    (*value_out).set_size(order_found.size_value);
    (*value_out).set_size_compressed(order_found.size_value_compressed);
  } else if (   found
             && order_found.type == OrderType::Delete) {
    s = Status::DeleteOrder();
  } else {
    s = Status::NotFound("Unable to find entry");
  }

  // exit the "copy" buffer
  log::debug("LOCK", "5 lock");
  mutex_copy_read_level5_.lock();
  num_readers_ -= 1;
  mutex_copy_read_level5_.unlock();
  log::debug("LOCK", "5 unlock");
  cv_read_.notify_one();

  return s;
}


Status WriteBuffer::Put(WriteOptions& write_options, Kitten& key, Kitten& chunk) {
  //return Write(OrderType::Put, key, value);
  return Status::InvalidArgument("WriteBuffer::Put() is not implemented");
}


Status WriteBuffer::PutChunk(WriteOptions& write_options,
                             Kitten& key,
                             Kitten& chunk,
                             uint64_t offset_chunk,
                             uint64_t size_value,
                             uint64_t size_value_compressed,
                             uint32_t crc32) {
  return WriteChunk(write_options,
                    OrderType::Put,
                    key,
                    chunk,
                    offset_chunk,
                    size_value,
                    size_value_compressed,
                    crc32
                   );
}


Status WriteBuffer::Delete(WriteOptions& write_options, Kitten& key) {
  auto empty = Kitten::NewEmptyKitten();
  return WriteChunk(write_options, OrderType::Delete, key, empty, 0, 0, 0, 0);
}


Status WriteBuffer::WriteChunk(const WriteOptions& write_options,
                               const OrderType& op,
                               Kitten& key,
                               Kitten& chunk,
                               uint64_t offset_chunk,
                               uint64_t size_value,
                               uint64_t size_value_compressed,
                               uint32_t crc32) {
  if (IsStopRequested()) return Status::IOError("Cannot handle request: WriteBuffer is closing");

  log::trace("WriteBuffer::WriteChunk()",
             "key:[%s] | size chunk:%" PRIu64 ", total size value:%" PRIu64 " offset_chunk:%" PRIu64 " sizeOfBuffer:%d",
             key.ToString().c_str(), chunk.size(), size_value, offset_chunk, buffers_[im_live_].size());

  bool is_first_chunk = (offset_chunk == 0);
  bool is_large = key.size() + size_value > db_options_.storage__hstable_size;

  uint64_t bytes_arriving = 0;
  if (is_first_chunk) bytes_arriving += key.size();
  bytes_arriving += chunk.size();

  if (UseRateLimiter()) rate_limiter_.Tick(bytes_arriving);

  // TODO: here the buffer index im_live_ is called outside of the level 2 and 3 mutexes is this really safe?
  log::debug("LOCK", "1 lock");
  std::unique_lock<std::mutex> lock_live(mutex_live_write_level1_);
  mutex_indices_level3_.lock();
  buffers_[im_live_].push_back(Order{std::this_thread::get_id(),
                                     write_options,
                                     op,
                                     key,
                                     chunk,
                                     offset_chunk,
                                     size_value,
                                     size_value_compressed,
                                     crc32,
                                     is_large});
  sizes_[im_live_] += bytes_arriving;
  uint64_t size_buffer_live = sizes_[im_live_];
  mutex_indices_level3_.unlock();

  /*
  if (buffers_[im_live_].size()) {
    for(auto &p: buffers_[im_live_]) {   
      log::trace("WriteBuffer::WriteChunk()",
                "Write() ITEM key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_live_]:%d",
                p.key, p.key->ToString().c_str(), p.chunk->size(), p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
    }
  } else {
    log::trace("WriteBuffer::WriteChunk()", "Write() ITEM no buffers_[im_live_]");
  }
  */

  if (size_buffer_live > buffer_size_) {
    log::trace("WriteBuffer::WriteChunk()", "trying to swap");
    mutex_flush_level2_.lock();
    log::debug("LOCK", "2 lock");
    log::debug("LOCK", "3 lock");
    std::unique_lock<std::mutex> lock_swap(mutex_indices_level3_);
    /*
    log::trace("WriteBuffer::WriteChunk()", "Swap buffers");
    std::swap(im_live_, im_copy_);
    */
    cv_flush_.notify_one();
    log::debug("LOCK", "3 unlock");
    mutex_flush_level2_.unlock();
    log::debug("LOCK", "2 unlock");

  } else {
    log::trace("WriteBuffer::WriteChunk()", "will not swap");
  }

  log::debug("LOCK", "1 unlock");
  return Status::OK();
}


void WriteBuffer::ProcessingLoop() {
  while(true) {
    log::trace("WriteBuffer", "ProcessingLoop() - start");
    log::debug("LOCK", "2 lock");
    std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
    while (sizes_[im_live_] == 0) {
      log::trace("WriteBuffer", "ProcessingLoop() - wait - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());
      std::cv_status status = cv_flush_.wait_for(lock_flush, std::chrono::milliseconds(db_options_.write_buffer__flush_timeout));
      if (IsStopRequestedAndBufferEmpty()) return;
    }

    mutex_indices_level3_.lock();
    if (sizes_[im_copy_] == 0) {
      std::swap(im_live_, im_copy_);
    }
    mutex_indices_level3_.unlock();

    log::trace("WriteBuffer", "ProcessingLoop() - start swap - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());
 
    // Notify the storage engine that the buffer can be flushed
    log::trace("BM", "WAIT: Get()-flush_buffer");

    if (UseRateLimiter()) rate_limiter_.WriteStart(); 
    event_manager_->flush_buffer.StartAndBlockUntilDone(buffers_[im_copy_]);

    // Wait for the index to notify the buffer manager
    log::trace("BM", "WAIT: Get()-clear_buffer");
    event_manager_->clear_buffer.Wait();
    event_manager_->clear_buffer.Done();
    
    // Wait for readers
    // TODO: the cleaning of the flush buffer shouldn't be done in one go but
    // in multiple iterations just like the transfer of indexes is being done,
    // so that the readers are never blocked for too long.
    log::debug("LOCK", "4 lock");
    mutex_copy_write_level4_.lock();
    while(true) {
      log::debug("LOCK", "5 lock");
      std::unique_lock<std::mutex> lock_read(mutex_copy_read_level5_);
      if (num_readers_ == 0) break;
      log::debug("WriteBuffer", "ProcessingLoop() - wait for lock_read");
      cv_read_.wait(lock_read);
    }
    log::debug("LOCK", "5 unlock");

    if (UseRateLimiter()) rate_limiter_.WriteEnd(sizes_[im_copy_]); 
    log::trace("WriteBuffer", "ProcessingLoop() bytes_in_buffer: %" PRIu64 " rate_writing: %" PRIu64, sizes_[im_copy_], rate_limiter_.GetWritingRate());

    // Clear flush buffer
    log::debug("WriteBuffer::ProcessingLoop()", "clear flush buffer");

    if (buffers_[im_copy_].size()) {
      for(auto &p: buffers_[im_copy_]) {
        log::trace("WriteBuffer", "ProcessingLoop() ITEM im_copy - key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_copy_]:%d", p.key.ToString().c_str(), p.chunk.size(), p.size_value, p.offset_chunk, buffers_[im_copy_].size(), sizes_[im_copy_]);
      }
    } else {
      log::trace("WriteBuffer", "ProcessingLoop() ITEM no buffers_[im_copy_]");
    }

    if (buffers_[im_live_].size()) {
      for(auto &p: buffers_[im_live_]) {
        log::trace("WriteBuffer", "ProcessingLoop() ITEM im_live - key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_live_]:%d", p.key.ToString().c_str(), p.chunk.size(), p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
      }
    } else {
      log::trace("WriteBuffer", "ProcessingLoop() ITEM no buffers_[im_live_]");
    }
    /*
    */

    /*
    for(auto &p: buffers_[im_copy_]) {
      delete p.key;
      delete p.chunk;
    }
    */
    sizes_[im_copy_] = 0;
    buffers_[im_copy_].clear();

    log::trace("WriteBuffer", "ProcessingLoop() - end swap - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());

    mutex_copy_write_level4_.unlock();
    log::debug("LOCK", "4 unlock");
    log::debug("LOCK", "2 unlock");
    cv_flush_done_.notify_all();

    if (IsStopRequestedAndBufferEmpty()) return;
  }
}

} // namespace kdb
