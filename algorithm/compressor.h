// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMPRESSOR_H_
#define KINGDB_COMPRESSOR_H_

#include <algorithm>
#include <map>

#include "algorithm/lz4.h"

#include "util/logger.h"
#include "util/status.h"
#include "thread/threadstorage.h"
#include "algorithm/crc32c.h"

namespace kdb {

class CompressorLZ4 {
 public:
  CompressorLZ4() {
  }

  // Added an empty copy assignment operator to avoid error messages of the type:
  // "object of type '...' cannot be assigned because its copy assignment
  //  operator is implicitly deleted"
  CompressorLZ4& operator=(const CompressorLZ4& r) {
    if(&r == this) return *this;
    return *this;
  }

  virtual ~CompressorLZ4() {
    //log::emerg("CompressorLZ4()::dtor", "call");
  }

  void ResetThreadLocalStorage();

  Status Compress(char *raw_in,
                  uint64_t size_raw_in,
                  char **compressed_out,
                  uint64_t *size_compressed_out
                 );

  Status Uncompress(char *source,
                    uint64_t size_source,
                    char **dest,
                    uint64_t *size_dest,
                    char **frame_out,
                    uint64_t *size_frame_out
                   );

  uint64_t thread_local_handler(std::map<std::thread::id, uint64_t>& status,
                                std::mutex& mutex,
                                uint64_t value,
                                bool apply);

  uint64_t size_compressed() { return ts_compress_.get(); }

  uint64_t MaxInputSize() {
    return LZ4_MAX_INPUT_SIZE;
  }

 private:
  ThreadStorage ts_compress_;
  ThreadStorage ts_uncompress_;
  CRC32 crc32_;
};

};

#endif // KINGDB_COMPRESSOR_H_
