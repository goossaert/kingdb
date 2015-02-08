// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "algorithm/compressor.h"

namespace kdb {

void CompressorLZ4::ResetThreadLocalStorage() {
  ts_compress_.reset();
  ts_uncompress_.reset();
}


Status CompressorLZ4::Compress(char *source,
                               uint64_t size_source,
                               char **dest,
                               uint64_t *size_dest) {
  if (size_source < 8) {
    *dest = nullptr;
    *size_dest = 0;
    return Status::OK();
  }
  uint32_t bound = LZ4_compressBound(size_source);
  *size_dest = 0;
  *dest = new char[8 + bound];

  int ret = LZ4_compress_limitedOutput(source, (*dest) + 8, size_source, bound);
  if (ret <= 0) {
    delete[] *dest;
    return Status::IOError("LZ4_compress_limitedOutput() failed");
  }
  uint32_t size_compressed = ret + 8;
  uint32_t size_compressed_stored = size_compressed;

  // If the frame was compressed to a size larger than the original data,
  // we just copy the original data.
  if (ret > size_source) {
    if (size_source > 8 + bound) {
      delete[] *dest;
      *dest = new char[8 + size_source];
    }
    memcpy((*dest) + 8, source, size_source);
    size_compressed = size_source + 8;
    size_compressed_stored = 0;
  }

  // NOTE: Yes, storing 64 bits into 32, but overflows will not happens as
  //       size_source is limited to db.storage.maximum_chunk_size
  uint32_t size_source_32 = size_source;
  EncodeFixed32((*dest),     size_compressed_stored);
  EncodeFixed32((*dest) + 4, size_source_32);
  // NOTE: small entries don't need to have the compressed and source sizes
  //       in front of the frame, this is just a waste of storage space.
  //       Maybe have a special type of entry, like 'small' or 'self-contained',
  //       which would indicate that the frame doesn't have the sizes.

  log::trace("CompressorLZ4::Compress()", "size_compressed:%u size_source:%u", size_compressed, size_source_32);
  uint64_t size_compressed_total = ts_compress_.get() + size_compressed;
  ts_compress_.put(size_compressed_total);
  *size_dest = size_compressed;
  return Status::OK();
}


bool CompressorLZ4::IsUncompressionDone(uint64_t size_source_total) {
  uint64_t offset_uncompress = ts_uncompress_.get();
  if (offset_uncompress == size_source_total) return true;
  return false;
}


Status CompressorLZ4::Uncompress(char *source,
                                 uint64_t size_source_total,
                                 char **dest,
                                 uint64_t *size_dest,
                                 char **frame_out,
                                 uint64_t *size_frame_out
                                 ) {
  uint64_t offset_uncompress = ts_uncompress_.get();
  log::trace("CompressorLZ4::Uncompress()", "in %" PRIu64 " %" PRIu64, offset_uncompress, size_source_total);
  *dest = nullptr;
  if (offset_uncompress == size_source_total) return Status::Done();

  uint32_t size_source, size_compressed;
  GetFixed32(source + offset_uncompress,     &size_compressed);
  GetFixed32(source + offset_uncompress + 4, &size_source);

  if (size_compressed > 0) {
    // the frame contains compressed data
    size_compressed -= 8;
    *size_dest = 0;
    *dest = new char[size_source];
    int size = size_compressed;
    log::trace("CompressorLZ4::Uncompress()", "ptr:%p size:%d size_source:%d offset:%" PRIu64, source + offset_uncompress + 8, size, size_source, offset_uncompress);
    int ret = LZ4_decompress_safe_partial(source + offset_uncompress + 8,
                                          *dest,
                                          size,
                                          size_source,
                                          size_source);
    if (ret <= 0) {
      delete[] (*dest);
      *dest = nullptr;
      return Status::IOError("LZ4_decompress_safe_partial() failed");
    }
    *size_dest = ret;
  } else {
    // the frame contains uncompressed data
    size_compressed = size_source;
    *size_dest = size_source;
    *dest = new char[size_source];
    memcpy(*dest, source, size_source);
  }

  crc32_.stream(source + offset_uncompress, size_compressed + 8);

  *frame_out = source + offset_uncompress;
  *size_frame_out = size_compressed + 8;
  log::trace("CompressorLZ4::Uncompress()", "crc32:0x%" PRIx64 " frame_ptr:%p frame_size:%" PRIu64, crc32_.get(), *frame_out, *size_frame_out);

  offset_uncompress += size_compressed + 8;
  ts_uncompress_.put(offset_uncompress);

  log::trace("CompressorLZ4::Uncompress()", "out %" PRIu64 " %" PRIu64, offset_uncompress, size_source_total);
  return Status::OK();
}

};
