// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

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

#include "util/compressor.h"
#include "util/crc32c.h"

namespace kdb {

class KingDB: public Interface {
 public:
  KingDB(const DatabaseOptions& db_options, const std::string dbname)
      : db_options_(db_options),
        dbname_(dbname),
        bm_(db_options),
        se_(db_options, dbname)
  {
    self_ = this;
    signal(SIGINT, SigIntHandlerStatic);
  }
  virtual ~KingDB() {}

  static void SigIntHandlerStatic(int signal) {
    KingDB::self_->SigIntHandler(signal);
  }

  void SigIntHandler(int signal) {
    se_.Close();
    exit(0);
  }

  virtual Status Get(ReadOptions& read_options, ByteArray* key, ByteArray** value_out) override;
  virtual Status Put(WriteOptions& write_options, ByteArray *key, ByteArray *chunk) override;
  virtual Status PutChunk(WriteOptions& write_options,
                          ByteArray *key,
                          ByteArray *chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) override;
  virtual Status Remove(WriteOptions& write_options, ByteArray *key) override;

 private:
  // TODO-6: Make sure that if multiple threads are creating KingDB objects with
  //         the same database name, they would all refer to the same buffer
  //         manager and storage engine.
  kdb::DatabaseOptions db_options_;
  std::string dbname_;
  std::mutex mutex_;
  kdb::BufferManager bm_;
  kdb::StorageEngine se_;
  kdb::CompressorLZ4 compressor_;
  kdb::CRC32 crc32_;
  static KingDB* self_;
};

};


#endif // KINGDB_INTERFACE_MAIN_H_
