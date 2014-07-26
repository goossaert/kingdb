// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_MAIN_H_
#define KINGDB_INTERFACE_MAIN_H_

#include <thread>
#include <string>
#include <memory>

#include "interface.h"
#include "buffer_manager.h"
#include "storage_engine.h"
#include "status.h"
#include "common.h"
#include "byte_array.h"


namespace kdb {

class KingDB: public Interface {
 public:
  KingDB(std::string dbname):
    dbname_(dbname),
    se_(dbname)
  {
  }
  virtual ~KingDB() {}

  virtual Status Get(ByteArray* key, ByteArray** value_out) override;
  virtual Status Put(ByteArray *key, ByteArray *chunk) override;
  virtual Status PutChunk(ByteArray *key,
                          ByteArray *chunk,
                          uint64_t offset_chunk,
                          uint64_t size_value) override;
  virtual Status Remove(ByteArray *key) override;

 private:
  std::string dbname_;
  std::mutex mutex_;
  kdb::BufferManager bm_;
  kdb::StorageEngine se_;
};

};


#endif // KINGDB_INTERFACE_MAIN_H_
