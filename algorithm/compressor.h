// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_COMPRESSOR_H_
#define KINGDB_COMPRESSOR_H_

#include "util/debug.h"

#include <algorithm>
#include <map>
#include <cinttypes>

#include "algorithm/lz4.h"

#include "util/logger.h"
#include "util/status.h"
#include "thread/threadstorage.h"
#include "algorithm/crc32c.h"

namespace kdb {

/* Depending on whether or not compression is enabled, and if compression is
 * even possible (i.e. the data is not incompressible), the data stored for an
 * entry will be stored with the appropriate format.
 *
 * 1. Compression is disabled
 * - The space reserved on secondary storage for the entry's value is of the
 *   exact size of the value itself, and the data of the value is stored
 *   contiguously in that space.
 * - The entry is marked as being 'compacted', because it uses exactly the space
 *   that was allocated for it.
 * - The checksum calculation is done over the raw data, i.e. the data of the
 *   value itself.
 *
 * 2. Compression is enabled
 * - Data is stored in 'frames'. Each frame contains a chunk of compressed data,
 *   with its own compression dictionary (depending on the compression algorithm
 *   being used).
 * - The space reserved on secondary storage for the entry's value is of the
 *   size of the raw value data, to which a padding is added. That padding must
 *   be at least as large as the size of a frame header, which is 8 bytes.
 * - The entry is marked as being 'uncompacted', the space reserved on secondary
 *   storage is likely to be more than the space the compressed value data is
 *   actually using (in the case of incompressible data, the allocated space may
 *   be just enough).
 * - Each frame is as follows:
 *   Frame header:
 *     * size_compressed: size of compressed data in bytes] (4 bytes, 32
 *       unsigned integer)
 *     * size_raw: size of raw data in bytes] (4 bytes, 32 unsigned integer)
 *   Frame data:
 *     * compressed data (of size 'size_compressed' bytes)
 * - Each chunk of data is compressed, and if the size of the output byte array
 *   is larger than the raw data, then the data is stored uncompressed: the
 *   compressed data in the frame simply contains the raw data itself. And the
 *   'size_compressed' field in the frame header is set to 0 to indicate that
 *   the data stored is the raw data.
 * - The checksum calculation is done over the frames, i.e. the compressed data
 *   along with the frame headers.
 *
 * 3. The data or part of the data is incompresssible
 * - If the data is incompressible, i.e. the compressed data takes more space
 *   than the raw data, then we have a problem. The solution is to allocate at
 *   least the size of one frame header, which guarantees that in the worst
 *   case, the can be stored as it is, uncompressed.
 * - As the compressed frames are being computed, the following check is
 *   performed:
 *     * size_frame_header = 8
 *     * size_remaining =   (total size of raw value)
 *                        - (current offset in the raw value)
 *       => size of raw data that is left to come
 *     * size_chunk
 *       => size of the chunk of raw value currently incoming
 *     * space_left =   (total size of raw value)
 *                    + (size of reserved padding)
 *                    - (offset where the next write will go on secondary storage)
 *       => size of the space left on secondary storage where data can be
 *          written for this entry.
 *     * size_compressed: size of the current chunk of data after it was
 *       compressed (including the frame header)
 *     * if ( size_remaining - size_chunk + size_frame_header
 *           > space_left - size_compressed) {
 *         - cancel the compression of the current chunk
 *         - disable compression for the rest of the streaming of this entry,
 *           and store the rest of the incoming data as raw.
 *       }
 *       => This if-statement is testing, considering that the current chunk
 *          is compressed, if there will be enough space to store the rest
 *          of the entry as raw data when the next chunk comes in, including
 *          a frame header. If not, then the compression needs to be disabled
 *          immediately. Given that the padding is at least as large as the
 *          size of a frame header, and that the test is performed over the
 *          next chunk, any point of the compression stream will have enough
 *          space to store the entry as raw data if it needs to.
 * - If compression is disabled mid-flight, the frame header is a sequence of
 *   null bytes. That way, the decompression process will be able to identify
 *   where compression was disabled, and copy the raw data directly on-ward.
 * */

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

  bool IsUncompressionDone(uint64_t size_source);
  Status Uncompress(char *source,
                    uint64_t size_source,
                    char **dest,
                    uint64_t *size_dest,
                    char **frame_out,
                    uint64_t *size_frame_out
                   );

  void DisableCompressionInFrameHeader(char* frame) {
    for (int i = 0; i < size_frame_header(); i++) frame[i] = 0;
  }

  bool HasFrameHeaderDisabledCompression(char *frame) {
    for (int i = 0; i < size_frame_header(); i++) {
      if(frame[i] != 0) return false;
    }
    return true;
  }

  uint64_t thread_local_handler(std::map<std::thread::id, uint64_t>& status,
                                std::mutex& mutex,
                                uint64_t value,
                                bool apply);

  uint64_t size_compressed() { return ts_compress_.get(); }

  uint64_t MaxInputSize() {
    return LZ4_MAX_INPUT_SIZE;
  }

  uint64_t size_frame_header() { return 8; };
  uint64_t size_uncompressed_frame(uint64_t size_data) { return size_data + 8; }

  void AdjustCompressedSize(int64_t inc) {
    int64_t size = ts_compress_.get() + inc;
    ts_compress_.put(size);
  }


 private:
  ThreadStorage ts_compress_;
  ThreadStorage ts_uncompress_;
  CRC32 crc32_;
};

};

#endif // KINGDB_COMPRESSOR_H_
