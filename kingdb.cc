// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "kingdb.h"

namespace kdb {

Status KingDB::Get(const std::string& key, Value** value_out) {
  LOG_TRACE("KingDB Get()", "[%s]", key.c_str());
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

Status KingDB::Put(const std::string& key, const std::string& value) {
  LOG_TRACE("KingDB Put()", "[%s]", key.c_str());
  return bm_.Put(key, value);
}


Status KingDB::PutChunk(const char* key,
                          uint64_t size_key,
                          const char* chunk,
                          uint64_t size_chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value,
                          char * buffer_to_delete) {
  LOG_TRACE("KingDB PutChunk()", "[%s] offset_chunk:%llu", key, offset_chunk);
  return bm_.PutChunk(key, size_key, chunk, size_chunk, offset_chunk, size_value, buffer_to_delete);
}


Status KingDB::Remove(const std::string& key) {
  LOG_TRACE("KingDB Remove()", "[%s]", key.c_str());
  return bm_.Remove(key);
}

};

