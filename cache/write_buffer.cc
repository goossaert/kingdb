// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "cache/write_buffer.h"

namespace kdb {

void WriteBuffer::Flush() {
  if (IsStopRequested()) return;
  log::debug("LOCK", "2 lock");
  std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
  // NOTE: Doing the flushing and waiting twice, in case the two buffers,
  // 'live' and 'copy', have items. This is a quick hack and a better
  // solution should be investigated.
  for (auto i = 0; i < 2; i++) {
    cv_flush_.notify_one();
    cv_flush_done_.wait_for(lock_flush, std::chrono::milliseconds(db_options_.write_buffer__close_timeout));
  }
  log::trace("WriteBuffer::Flush()", "end");
}

Status WriteBuffer::Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) {
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
    if (*order.key == *key) {
      found = true;
      order_found = order;
    }
  }
  if (found) {
    log::debug("WriteBuffer::Get()", "found in buffer_live");
    if (   order_found.type == OrderType::Put
        && order_found.chunk->size() == order_found.size_value) {
      *value_out = order_found.chunk;
      return Status::OK();
    } else if (order_found.type == OrderType::Remove) {
      return Status::RemoveOrder();
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
    if (*order.key == *key) {
      found = true;
      order_found = order;
    }
  }

  Status s;
  if (found) log::debug("WriteBuffer::Get()", "found in buffer_copy");
  if (   found
      && order_found.type == OrderType::Put
      && order_found.chunk->size() == order_found.size_value) {
    *value_out = order_found.chunk;
    s = Status::OK();
  } else if (   found
             && order_found.type == OrderType::Remove) {
    s = Status::RemoveOrder();
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


Status WriteBuffer::Put(WriteOptions& write_options, ByteArray* key, ByteArray* chunk) {
  //return Write(OrderType::Put, key, value);
  return Status::InvalidArgument("WriteBuffer::Put() is not implemented");
}


Status WriteBuffer::PutChunk(WriteOptions& write_options,
                               ByteArray* key,
                               ByteArray* chunk,
                               uint64_t offset_chunk,
                               uint64_t size_value,
                               uint64_t size_value_compressed,
                               uint32_t crc32) {
  return WriteChunk(OrderType::Put,
                    key,
                    chunk,
                    offset_chunk,
                    size_value,
                    size_value_compressed,
                    crc32
                   );
}


Status WriteBuffer::Remove(WriteOptions& write_options, ByteArray* key) {
  // TODO: The storage engine is calling data() and size() on the chunk ByteArray.
  //       The use of SimpleByteArray here is a hack to guarantee that data()
  //       and size() won't be called on a nullptr -- this needs to be cleaned up.
  auto empty_chunk = new SimpleByteArray(nullptr, 0);
  return WriteChunk(OrderType::Remove, key, empty_chunk, 0, 0, 0, 0);
}


Status WriteBuffer::WriteChunk(const OrderType& op,
                                 ByteArray* key,
                                 ByteArray* chunk,
                                 uint64_t offset_chunk,
                                 uint64_t size_value,
                                 uint64_t size_value_compressed,
                                 uint32_t crc32) {
  if (IsStopRequested()) return Status::IOError("Cannot handle request: WriteBuffer is closing");
  log::debug("LOCK", "1 lock");
  std::unique_lock<std::mutex> lock_live(mutex_live_write_level1_);

  log::trace("WriteBuffer::WriteChunk()",
            "Write() key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d",
            key->ToString().c_str(), chunk->size(), size_value, offset_chunk, buffers_[im_live_].size());

  bool is_first_chunk = (offset_chunk == 0);
  bool is_large = key->size() + size_value > db_options_.storage__hstable_size;
  buffers_[im_live_].push_back(Order{std::this_thread::get_id(),
                                     op,
                                     key,
                                     chunk,
                                     offset_chunk,
                                     size_value,
                                     size_value_compressed,
                                     crc32,
                                     is_large});

  if (is_first_chunk) {
    sizes_[im_live_] += key->size();
  }
  sizes_[im_live_] += chunk->size();

  if (buffers_[im_live_].size()) {
    for(auto &p: buffers_[im_live_]) {   
      log::trace("WriteBuffer::WriteChunk()",
                "Write() ITEM key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_live_]:%d",
                p.key, p.key->ToString().c_str(), p.chunk->size(), p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
    }
  } else {
    log::trace("WriteBuffer::WriteChunk()", "Write() ITEM no buffers_[im_live_]");
  }
  /*
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
  /*
  if (   chunk->size() + offset_chunk == size_value
      && offset_chunk > 0) {
    force_swap_ = true;
  }
  */

  // test on size for debugging remove()
  if (buffers_[im_live_].size() > 256) {
    // TODO: make this value optional -- a good default value would be the
    //       number of client threads.
    // NOTE: this is only here for when the database is used through a
    //       networking interface, in which case a better solution would be to not
    //       force a swap when the buffer reach a certain size, but just throttle the
    //       incoming requests.
    force_swap_ = true;
  }
  /*
  */

  if (sizes_[im_live_] > buffer_size_ || force_swap_) {
    log::trace("WriteBuffer::WriteChunk()", "trying to swap");
    // TODO: play with the mutex_flush_, try to keep it before the
    // if(can_swap_) or inside the if(can_swap_)
    log::debug("LOCK", "2 lock");
    std::unique_lock<std::mutex> lock_flush(mutex_flush_level2_);
    if (can_swap_) {
      log::trace("WriteBuffer::WriteChunk()", "can_swap_ == true");
      log::debug("LOCK", "3 lock");
      std::unique_lock<std::mutex> lock_swap(mutex_indices_level3_);
      log::trace("WriteBuffer::WriteChunk()", "Swap buffers");
      can_swap_ = false;
      force_swap_ = false;
      std::swap(im_live_, im_copy_);
      cv_flush_.notify_one();
      log::debug("LOCK", "3 unlock");
    } else {
      log::trace("WriteBuffer::WriteChunk()", "can_swap_ == false");
    }
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
    while (sizes_[im_copy_] == 0) {
      log::trace("WriteBuffer", "ProcessingLoop() - wait - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());
      can_swap_ = true;
      std::cv_status status = cv_flush_.wait_for(lock_flush, std::chrono::milliseconds(db_options_.write_buffer__flush_timeout));
      if (status == std::cv_status::no_timeout) {
        //log::info("WriteBuffer", "ProcessingLoop() - swapped no timeout");
        break;
      } else if (   status == std::cv_status::timeout
                 && buffers_[im_live_].size() > 0) {
        // Note: I could have made it so the swap only happened here and not in
        //       WriteChunk(), however it is simpler to have swapping code twice
        //       than to have to deal with adding and removing items from the
        //       live buffer, because it would requires lots of locking --
        //       working with the copy buffer is simpler.
        //log::info("WriteBuffer", "ProcessingLoop() - swapped timeout");
        std::unique_lock<std::mutex> lock_swap(mutex_indices_level3_);
        can_swap_ = false;
        force_swap_ = false;
        std::swap(im_live_, im_copy_);
        break;
      } else if (IsStopRequested()) {
        return;
      }
    }

    log::trace("WriteBuffer", "ProcessingLoop() - start swap - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());
 
    // Notify the storage engine that the buffer can be flushed
    log::trace("BM", "WAIT: Get()-flush_buffer");
    event_manager_->flush_buffer.StartAndBlockUntilDone(buffers_[im_copy_]);

    // Wait for the index to notify the buffer manager
    log::trace("BM", "WAIT: Get()-clear_buffer");
    event_manager_->clear_buffer.Wait();
    event_manager_->clear_buffer.Done();
    
    // Wait for readers
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

    // Clear flush buffer
    log::debug("WriteBuffer::ProcessingLoop()", "clear flush buffer");
    for(auto &p: buffers_[im_copy_]) {
      delete p.key;
      delete p.chunk;
    }
    sizes_[im_copy_] = 0;
    buffers_[im_copy_].clear();

    log::trace("WriteBuffer", "ProcessingLoop() - end swap - %" PRIu64 " %" PRIu64, buffers_[im_copy_].size(), buffers_[im_live_].size());
 
    if (buffers_[im_copy_].size()) {
      for(auto &p: buffers_[im_copy_]) {
        log::trace("WriteBuffer", "ProcessingLoop() ITEM im_copy - key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_copy_]:%d", p.key, p.key->ToString().c_str(), p.chunk->size(), p.size_value, p.offset_chunk, buffers_[im_copy_].size(), sizes_[im_copy_]);
      }
    } else {
      log::trace("WriteBuffer", "ProcessingLoop() ITEM no buffers_[im_copy_]");
    }

    if (buffers_[im_live_].size()) {
      for(auto &p: buffers_[im_live_]) {
        log::trace("WriteBuffer", "ProcessingLoop() ITEM im_live - key_ptr:[%p] key:[%s] | size chunk:%d, total size value:%d offset_chunk:%" PRIu64 " sizeOfBuffer:%d sizes_[im_live_]:%d", p.key, p.key->ToString().c_str(), p.chunk->size(), p.size_value, p.offset_chunk, buffers_[im_live_].size(), sizes_[im_live_]);
      }
    } else {
      log::trace("WriteBuffer", "ProcessingLoop() ITEM no buffers_[im_live_]");
    }

    can_swap_ = true;
    mutex_copy_write_level4_.unlock();
    log::debug("LOCK", "4 unlock");
    log::debug("LOCK", "2 unlock");
    cv_flush_done_.notify_all();
  }
}

} // namespace kdb
