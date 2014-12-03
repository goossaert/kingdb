// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "interface/kingdb.h"

namespace kdb {

Status KingDB::Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) {
  log::trace("KingDB Get()", "[%s]", key->ToString().c_str());
  Status s = wb_->Get(read_options, key, value_out);
  if (s.IsRemoveOrder()) {
    return Status::NotFound("Unable to find entry");
  } else if (s.IsNotFound()) {
    log::trace("KingDB Get()", "not found in buffer");
    s = se_->Get(key, value_out);
    if (s.IsNotFound()) {
      log::trace("KingDB Get()", "not found in storage engine");
      return s;
    } else if (s.IsOK()) {
      log::trace("KingDB Get()", "found in storage engine");
      return s;
    } else {
      log::trace("KingDB Get()", "unidentified error");
      return s;
    }
  }

  log::trace("KingDB Get()", "found in buffer");
  return s;
}


Status KingDB::Put(WriteOptions& write_options, ByteArray *key, ByteArray *chunk) {
  return PutChunk(write_options, key, chunk, 0, chunk->size());
}


Status KingDB::PutChunk(WriteOptions& write_options,
                        ByteArray *key,
                        ByteArray *chunk,
                        uint64_t offset_chunk,
                        uint64_t size_value) {
  if (size_value <= db_options_.storage__maximum_chunk_size) {
    return PutChunkValidSize(write_options, key, chunk, offset_chunk, size_value);
  }

  // 'chunk' may be deleted by the call to PutChunkValidSize()
  // and therefore it cannot be used in the loop test condition
  uint64_t size_chunk = chunk->size(); 
  Status s;
  for (uint64_t offset = 0; offset < size_chunk; offset += db_options_.storage__maximum_chunk_size) {
    ByteArray *chunk_new;
    if (offset + db_options_.storage__maximum_chunk_size < chunk->size()) {
      chunk_new = new SimpleByteArray(chunk->data() + offset,
                                      db_options_.storage__maximum_chunk_size);
    } else {
      chunk_new = chunk;
      chunk_new->set_offset(offset);
    }
    s = PutChunkValidSize(write_options, key, chunk_new, offset_chunk + offset, size_value);
    if (!s.IsOK()) break;
  }

  return s;
}


Status KingDB::PutChunkValidSize(WriteOptions& write_options,
                                 ByteArray *key,
                                 ByteArray *chunk,
                                 uint64_t offset_chunk,
                                 uint64_t size_value) {
  Status s;
  s = se_->FileSystemStatus();
  if (!s.IsOK()) return s;
  log::trace("KingDB::PutChunkValidSize()",
            "[%s] offset_chunk:%" PRIu64,
            key->ToString().c_str(),
            offset_chunk);

  bool do_compression = true;
  uint64_t size_value_compressed = 0;
  uint64_t offset_chunk_compressed = offset_chunk;
  ByteArray *chunk_final = nullptr;
  SharedAllocatedByteArray *chunk_compressed = nullptr;

  bool is_first_chunk = (offset_chunk == 0);
  bool is_last_chunk = (chunk->size() + offset_chunk == size_value);
  log::trace("KingDB::PutChunkValidSize()",
            "CompressionType:%d",
            db_options_.compression.type);

  if (   chunk->size() == 0
      || db_options_.compression.type == kNoCompression) {
    do_compression = false;
  }

  if (!do_compression) {
    chunk_final = chunk;
  } else {
    if (is_first_chunk) {
      compressor_.ResetThreadLocalStorage();
    }

    log::trace("KingDB::PutChunkValidSize()",
              "[%s] size_compressed:%" PRIu64,
              key->ToString().c_str(), compressor_.size_compressed());

    offset_chunk_compressed = compressor_.size_compressed();

    uint64_t size_compressed;
    char *compressed;
    s = compressor_.Compress(chunk->data(),
                                    chunk->size(),
                                    &compressed,
                                    &size_compressed);
    if (!s.IsOK()) return s;
    chunk_compressed = new SharedAllocatedByteArray(compressed, size_compressed);

    log::trace("KingDB::PutChunkValidSize()",
              "[%s] (%" PRIu64 ") compressed size %" PRIu64 " - offset_chunk_compressed %" PRIu64,
              key->ToString().c_str(),
              chunk->size(),
              chunk_compressed->size(),
              offset_chunk_compressed);

    if (is_last_chunk) {
      size_value_compressed = compressor_.size_compressed();
    }

    chunk_final = chunk_compressed;
    delete chunk;
  }

  // Compute CRC32 checksum
  uint32_t crc32 = 0;
  if (is_first_chunk) {
    crc32_.ResetThreadLocalStorage();
    crc32_.stream(key->data(), key->size());
  }
  crc32_.stream(chunk_final->data(), chunk_final->size());
  if (is_last_chunk) crc32 = crc32_.get();

  log::trace("KingDB PutChunkValidSize()", "[%s] size_value_compressed:%" PRIu64 " crc32:0x%" PRIx64 " END", key->ToString().c_str(), size_value_compressed, crc32);

  return wb_->PutChunk(write_options,
                      key,
                      chunk_final,
                      offset_chunk_compressed,
                      size_value,
                      size_value_compressed,
                      crc32);
}


Status KingDB::Remove(WriteOptions& write_options,
                      ByteArray *key) {
  log::trace("KingDB::Remove()", "[%s]", key->ToString().c_str());
  Status s = se_->FileSystemStatus();
  if (!s.IsOK()) return s;
  return wb_->Remove(write_options, key);
}


Interface* KingDB::NewSnapshot() {
  log::trace("KingDB::NewSnapshot()", "start");
  std::set<uint32_t>* fileids_ignore;
  uint32_t snapshot_id;
  Status s = se_->GetNewSnapshotData(&snapshot_id, &fileids_ignore);
  if (!s.IsOK()) return nullptr;

  log::trace("KingDB::NewSnapshot()", "Flushing 0");
  wb_->Flush();
  log::trace("KingDB::NewSnapshot()", "Flushing 1");
  uint32_t fileid_end = se_->FlushCurrentFileForSnapshot();
  log::trace("KingDB::NewSnapshot()", "Flushing 2");
  StorageEngine *se_readonly = new StorageEngine(db_options_,
                                                 nullptr,
                                                 dbname_,
                                                 true,
                                                 fileids_ignore,
                                                 fileid_end);
  std::vector<uint32_t> *fileids_iterator = se_readonly->GetFileidsIterator();
  Snapshot *snapshot = new Snapshot(db_options_,
                                    dbname_,
                                    se_,
                                    se_readonly,
                                    fileids_iterator,
                                    snapshot_id);
  return snapshot;
}

} // namespace kdb
