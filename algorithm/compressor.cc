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
  /*
  if (size_source < 8) {
    *dest = nullptr;
    *size_dest = 0;
    return Status::OK();
  }
  */
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
  if ((uint64_t)ret > size_source) {
    if (size_source > 8 + bound) {
      delete[] *dest;
      *dest = new char[8 + size_source];
    }
    memcpy((*dest) + 8, source, size_source);
    size_compressed = size_source + 8;
    size_compressed_stored = 0;
  }

  // NOTE: Yes, storing 64 bits into 32, but overflows will not happens as
  //       size_source is limited to db.storage.maximum_part_size
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
                                 uint64_t *size_frame_out,
                                 bool do_memory_allocation // = true by dafault
                                 ) {
  uint64_t offset_uncompress = ts_uncompress_.get();
  log::trace("CompressorLZ4::Uncompress()", "in %" PRIu64 " %" PRIu64, offset_uncompress, size_source_total);
  if (do_memory_allocation) {
    *dest = nullptr;
  }
  if (offset_uncompress == size_source_total) return Status::Done();

  uint32_t size_source, size_compressed;
  GetFixed32(source + offset_uncompress,     &size_compressed);
  GetFixed32(source + offset_uncompress + 4, &size_source);

  if (size_compressed > 0) {
    // the frame contains compressed data
    size_compressed -= 8;
    *size_dest = 0;
    if (do_memory_allocation) {
      *dest = new char[size_source];
    }
    int size = size_compressed;
    log::trace("CompressorLZ4::Uncompress()", "ptr:%p size:%d size_source:%d offset:%" PRIu64, source + offset_uncompress + 8, size, size_source, offset_uncompress);
    int ret = LZ4_decompress_safe_partial(source + offset_uncompress + 8,
                                          *dest,
                                          size,
                                          size_source,
                                          size_source);
    if (ret <= 0) {
      if (do_memory_allocation) {
        delete[] (*dest);
        *dest = nullptr;
      }
      return Status::IOError("LZ4_decompress_safe_partial() failed");
    }
    *size_dest = ret;
  } else {
    // the frame contains uncompressed data
    size_compressed = size_source;
    *size_dest = size_source;
    if (do_memory_allocation) {
      *dest = new char[size_source];
    }
    memcpy(*dest, source + offset_uncompress + 8, size_source);
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


Status CompressorLZ4::UncompressByteArray(ByteArray& value,
                                          bool do_checksum_verification,
                                          ByteArray* value_uncompressed) {
  Status s;
  if (do_checksum_verification) {
    crc32_.ResetThreadLocalStorage();
    crc32_.put(value.checksum_initial()); 
  }
  bool is_compressed = value.is_compressed();
  bool is_compression_disabled = false;
  uint64_t offset_in = 0;
  uint64_t offset_out = 0;
  ResetThreadLocalStorage();

  *value_uncompressed = ByteArray::NewAllocatedMemoryByteArray(value.size());
  value_uncompressed->set_size(value.size());
  value_uncompressed->set_size_compressed(0);

  while (true) {

    if (is_compressed && !is_compression_disabled) {

      if (IsUncompressionDone(value.size_compressed())) {
        if (   !do_checksum_verification
            || crc32_.get() == value.checksum()) {
          log::debug("CompressorLZ4::UncompressByteArray()", "Good CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", value.checksum(), crc32_.get());
          return Status::OK();
        } else {
          log::debug("CompressorLZ4::UncompressByteArray()", "Bad CRC32 - stored:0x%08" PRIx64 " computed:0x%08" PRIx64 "\n", value.checksum(), crc32_.get());
          return Status::IOError("Invalid checksum.");
        }
      }

      if (HasFrameHeaderDisabledCompression(value.data() + offset_in)) {
        log::debug("CompressorLZ4::UncompressByteArray()", "Finds that compression is disabled\n");
        is_compression_disabled = true;
        if (do_checksum_verification) {
          crc32_.stream(value.data() + offset_in, size_frame_header());
        }
        offset_in += size_frame_header();
      }

      if (!is_compression_disabled) {
        char *frame;
        uint64_t size_frame;
        uint64_t size_out;
        char *buffer_out = value_uncompressed->data() + offset_out;

        log::trace("CompressorLZ4::UncompressByteArray()", "before uncompress");
        Status s = Uncompress(value.data(),
                              value.size_compressed(),
                              &buffer_out,
                              &size_out,
                              &frame,
                              &size_frame,
                              false);
        //chunk_ = NewShallowCopyByteArray(data_out, size_out);

        if (s.IsDone()) {
          return Status::OK();
        } else if (s.IsOK()) {
          if (do_checksum_verification) {
            crc32_.stream(frame, size_frame);
          }
        } else {
          return s;
        }

        offset_in += size_frame;
        offset_out += size_out;
      }
    }

    if (!is_compressed || is_compression_disabled) {
      log::trace("CompressorLZ4::UncompressByteArray()", "No compression or compression disabled");
      uint64_t size_left;
      if (is_compressed && is_compression_disabled) {
        size_left = value.size_compressed();
      } else {
        size_left = value.size();
      }

      if (offset_in == size_left) {
        log::trace("CompressorLZ4::UncompressByteArray()", "Has gotten all the data");
        return Status::OK();
      }

      char* data_left = value.data() + offset_in;

      size_t step = 1024*1024;
      size_t size_current = offset_in + step < size_left ? step : size_left - offset_in;
      if (do_checksum_verification) {
        crc32_.stream(data_left, size_current);
      }

      memcpy(value_uncompressed->data() + offset_out, data_left, size_current);

      //chunk_ = value;
      //chunk_.increment_offset(offset);
      //chunk_.set_size(size_current);
      //chunk_.set_size_compressed(0);
      offset_in += size_current;
      offset_out += size_current;
      log::trace("CompressorLZ4::UncompressByteArray()", "Done with handling uncompressed data");
      return Status::OK();
    }
  }
  return true;

}

};
