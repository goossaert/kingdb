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
  return bm_.PutChunk(key, chunk, offset_chunk, size_value);
}


Status KingDB::Remove(ByteArray *key) {
  LOG_TRACE("KingDB Remove()", "[%s]", key->ToString().c_str());
  return bm_.Remove(key);
}

};

