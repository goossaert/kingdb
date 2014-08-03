// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "kingdb.h"

namespace kdb {

Status KingDB::Get(ByteArray* key, ByteArray** value_out) {
  LOG_TRACE("KingDB Get()", "[%s]", key->ToString().c_str());
  Status s = bm_.Get(key, value_out);
  if (s.IsRemoveOrder()) {
    return Status::NotFound("Unable to find entry");
  } else if (s.IsNotFound()) {
    LOG_TRACE("KingDB Get()", "not found in buffer");
    s = se_.Get(key, value_out);
    if (s.IsNotFound()) {
      LOG_TRACE("KingDB Get()", "not found in storage engine");
      return s;
    } else if (s.IsOK()) {
      LOG_TRACE("KingDB Get()", "found in storage engine");
      return s;
    } else {
      LOG_TRACE("KingDB Get()", "unidentified error");
      return s;
    }
  }

  LOG_TRACE("KingDB Get()", "found in buffer");
  return s;
}


Status KingDB::Put(ByteArray *key, ByteArray *chunk) {
  return PutChunk(key, chunk, 0, chunk->size());
}

Status KingDB::PutChunk(ByteArray *key,
                        ByteArray *chunk,
                        uint64_t offset_chunk,
                        uint64_t size_value) {
  LOG_TRACE("KingDB PutChunk()", "[%s] offset_chunk:%llu", key->ToString().c_str(), offset_chunk);
  bool do_compression = true;
  uint64_t size_value_compressed = 0;
  uint64_t offset_chunk_compressed = offset_chunk;
  ByteArray *chunk_final = nullptr;
  SharedAllocatedByteArray *chunk_compressed = nullptr;

  if (chunk->size() == 0) do_compression = false;

  if (do_compression) {
    if (offset_chunk == 0) {
      compressor_.Reset();
    }

    LOG_TRACE("KingDB PutChunk()", "[%s] size_compressed:%llu", key->ToString().c_str(), compressor_.size_compressed());

    offset_chunk_compressed = compressor_.size_compressed();

    uint64_t size_compressed;
    char *compressed;
    // BUG: the compressor_ is shared among multiple threads, which is why the
    // multi-threading uncompressing fails
    // The simplest solution is to do the summing of the size_compressed
    // outside of the compressor, to make sure that multiple threads won't
    // modify the compressor concurrently.
    Status s = compressor_.Compress(chunk->data(),
                                    chunk->size(),
                                    &compressed,
                                    &size_compressed);
    if (!s.IsOK()) return s;
    chunk_compressed = new SharedAllocatedByteArray(compressed, size_compressed);

    LOG_TRACE("KingDB PutChunk()", "[%s] (%llu) compressed size %llu - offset_chunk_compressed %llu", key->ToString().c_str(), chunk->size(), chunk_compressed->size(), offset_chunk_compressed);

    // When the last chunk is reached, set size_value_compressed to that
    // it can be passed to the storage engine
    if (chunk->size() + offset_chunk == size_value) {
      size_value_compressed = compressor_.size_compressed();
    }

    chunk_final = chunk_compressed;
    delete chunk;
  }

  if (!do_compression) {
    chunk_final = chunk;
  }

  LOG_TRACE("KingDB PutChunk()", "[%s] size_compressed:%llu END", key->ToString().c_str(), compressor_.size_compressed());

  return bm_.PutChunk(key,
                      chunk_final,
                      offset_chunk_compressed,
                      size_value,
                      size_value_compressed);
}


Status KingDB::Remove(ByteArray *key) {
  LOG_TRACE("KingDB Remove()", "[%s]", key->ToString().c_str());
  return bm_.Remove(key);
}

};

