// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMPRESSOR_H_
#define KINGDB_COMPRESSOR_H_

#include <algorithm>
#include <mutex>
#include <thread>
#include <map>

#include "lz4.h"

#include "common.h"
#include "logger.h"
#include "status.h"

namespace kdb {

class CompressorLZ4 {
 public:
  CompressorLZ4() {
    Reset();
  }

  // Added an empty copy assignment operator to avoid error messages of the type:
  // "object of type '...' cannot be assigned because its copy assignment
  //  operator is implicitly deleted"
  CompressorLZ4& operator=(const CompressorLZ4& r) {
    if(&r == this) return *this;
    return *this;
  }

  virtual ~CompressorLZ4() {
  }

  void Reset();

  Status Compress(char *raw_in,
                  uint64_t size_raw_in,
                  char **compressed_out,
                  uint64_t *size_compressed_out
                 );

  Status Uncompress(char *source,
                    uint64_t size_source,
                    char **dest,
                    uint64_t *size_dest
                   );

  uint64_t thread_local_handler(std::map<std::thread::id, uint64_t>& status,
                                std::mutex& mutex,
                                uint64_t value,
                                bool apply);

  uint64_t get_thread_local_var_compress();
  void put_thread_local_var_compress(uint64_t value);
  uint64_t get_thread_local_var_uncompress();
  void put_thread_local_var_uncompress(uint64_t value);
  uint64_t size_compressed() { return get_thread_local_var_compress(); }

 private:
  //uint64_t offset_uncompress_;
  //uint64_t size_compressed_;
  std::mutex mutex_compress_;
  std::mutex mutex_uncompress_;
  std::map<std::thread::id, uint64_t> status_compress_;
  std::map<std::thread::id, uint64_t> status_uncompress_;
};

};

#endif // KINGDB_COMPRESSOR_H_
