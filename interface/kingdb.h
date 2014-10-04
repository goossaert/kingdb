// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

#include <assert.h>
#include <thread>
#include <string>
#include <memory>

#include "kingdb/interface.h"
#include "buffer/buffer_manager.h"
#include "storage/storage_engine.h"
#include "util/status.h"
#include "kingdb/common.h"
#include "kingdb/byte_array.h"
#include "kingdb/options.h"
#include "interface/iterator.h"
#include "interface/snapshot.h"

#include "util/compressor.h"
#include "util/crc32c.h"
#include "util/endian.h"

namespace kdb {

// TODO: Add Flush() method?

class KingDB: public Interface {
 public:
  KingDB(const DatabaseOptions& db_options, const std::string dbname)
      : db_options_(db_options),
        dbname_(dbname),
        bm_(db_options),
        se_(db_options, dbname),
        is_closed_(false)
  {
    // Word-swapped endianness is not supported
    assert(getEndianness() == kBytesLittleEndian || getEndianness() == kBytesBigEndian);
  }

  virtual ~KingDB() {
    Close();
  }

  virtual void Close() override {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_closed_) return;
    is_closed_ = true;
    bm_.Close();
    se_.Close();
  }

  virtual Status Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) override;
  virtual Status Put(WriteOptions& write_options, ByteArray *key, ByteArray *chunk) override;
  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray *key,
                          ByteArray *chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) override;
  virtual Status Remove(WriteOptions& write_options, ByteArray *key) override;
  virtual Interface* NewSnapshot() override;
  virtual Iterator* NewIterator(ReadOptions& read_options) override { return nullptr; };

 private:
  // TODO-6: Make sure that if multiple threads are creating KingDB objects with
  //         the same database name, they would all refer to the same buffer
  //         manager and storage engine.
  kdb::DatabaseOptions db_options_;
  std::string dbname_;
  kdb::BufferManager bm_;
  kdb::StorageEngine se_;
  kdb::CompressorLZ4 compressor_;
  kdb::CRC32 crc32_;
  bool is_closed_;
  std::mutex mutex_close_;
};

};


#endif // KINGDB_INTERFACE_MAIN_H_
