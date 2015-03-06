// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_HSTABLE_MANAGER_H_
#define KINGDB_HSTABLE_MANAGER_H_

#include "util/debug.h"
#include <thread>
#include <mutex>
#include <chrono>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <cstdio>
#include <inttypes.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>

#include "kingdb/kdb.h"
#include "util/options.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/file.h"
#include "algorithm/crc32c.h"
#include "algorithm/hash.h"
#include "storage/format.h"
#include "storage/resource_manager.h"


namespace kdb {

// A HSTable (Hashed String Table) is a file consisting of entries, followed by
// an Offset Array. The entries are a sequence of bytes in the form <key, value>,
// and for each entry, the Offset Array has one item which is the hashed key of
// that entry, and the offset where the entry can be found in the file.
// The Offset Array can be used to quickly build a hash table in memory,
// mapping hashed keys to locations in HSTables.
class HSTableManager {
 public:
  HSTableManager() {
    is_closed_ = true;
    is_read_only_ = true;
    has_file_ = false;
    buffer_has_items_ = false;
    has_sync_option_ = false;
  }

  HSTableManager(DatabaseOptions& db_options,
                 std::string dbname,
                 std::string prefix,
                 std::string prefix_compaction,
                 std::string dirpath_locks,
                 FileType filetype_default,
                 bool read_only=false)
      : db_options_(db_options),
        is_read_only_(read_only),
        filetype_default_(filetype_default),
        prefix_(prefix),
        prefix_compaction_(prefix_compaction),
        dirpath_locks_(dirpath_locks),
        wait_until_can_open_new_files_(false) {
    log::trace("HSTableManager::HSTableManager()", "dbname:%s prefix:%s", dbname.c_str(), prefix.c_str());
    dbname_ = dbname;
    hash_ = MakeHash(db_options.hash);
    Reset();
    if (!is_read_only_) {
      buffer_raw_ = new char[size_block_*2];
      buffer_index_ = new char[size_block_*2];
    }
  }

  ~HSTableManager() {
    Close();
  }

  void Reset() {
    file_resource_manager.Reset();
    sequence_fileid_ = 0;
    sequence_timestamp_ = 0;
    size_block_ = db_options_.storage__hstable_size;
    has_file_ = false;
    buffer_has_items_ = false;
    is_closed_ = false;
    is_locked_sequence_timestamp_ = false;
    offset_start_ = 0;
    offset_end_ = 0;
  }

  void Close() {
    std::unique_lock<std::mutex> lock(mutex_close_);
    if (is_read_only_ || is_closed_) return;
    is_closed_ = true;
    FlushCurrentFile();
    CloseCurrentFile();
    delete hash_;
    if (!is_read_only_) {
      delete[] buffer_raw_;
      delete[] buffer_index_;
    }
  }

  std::string GetPrefix() {
    return prefix_;
  }

  std::string GetFilepath(uint32_t fileid) {
    return dbname_ + "/" + prefix_ + HSTableManager::num_to_hex(fileid); // TODO: optimize here
  }

  std::string GetLockFilepath(uint32_t fileid) {
    return dirpath_locks_ + "/" + HSTableManager::num_to_hex(fileid); // TODO: optimize here
  }

  // File id sequence helpers
  void SetSequenceFileId(uint32_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    sequence_fileid_ = seq;
    log::trace("HSTableManager::SetSequenceFileId", "seq:%u", seq);
  }

  uint32_t GetSequenceFileId() {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    return sequence_fileid_;
  }

  uint32_t IncrementSequenceFileId(uint32_t inc) {
    std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
    log::trace("HSTableManager::IncrementSequenceFileId", "sequence_fileid_:%u, inc:%u", sequence_fileid_, inc);
    sequence_fileid_ += inc;
    return sequence_fileid_;
  }


  // Timestamp sequence helpers
  void SetSequenceTimestamp(uint32_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    if (!is_locked_sequence_timestamp_) sequence_timestamp_ = seq;
  }

  uint64_t GetSequenceTimestamp() {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    return sequence_timestamp_;
  }

  uint64_t IncrementSequenceTimestamp(uint64_t inc) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    if (!is_locked_sequence_timestamp_) sequence_timestamp_ += inc;
    return sequence_timestamp_;
  }

  void LockSequenceTimestamp(uint64_t seq) {
    std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
    is_locked_sequence_timestamp_ = true;
    sequence_timestamp_ = seq;
  }


  static std::string num_to_hex(uint64_t num) {
    char buffer[20];
    sprintf(buffer, "%08" PRIx64, num);
    return std::string(buffer);
  }
 
  static uint32_t hex_to_num(char* hex) {
    uint32_t num;
    sscanf(hex, "%x", &num);
    return num;
  }

  uint32_t GetHighestStableFileId(uint32_t fileid_start) {
    // TODO: Extract the HSTable repair logic out of this method. This method
    // should only be computing the highest stable file id, and not do anything
    // else than that. I took this implementation shortcut to get the first beta
    // version out asap, this needs to be cleaned up at some point.
    uint32_t fileid_max = GetSequenceFileId();
    uint32_t fileid_stable = 0;
    uint32_t fileid_candidate = fileid_start;
    uint64_t epoch_now = file_resource_manager.GetEpochNow();
      
    while (true) {
      if (fileid_candidate >= fileid_max) break;
      uint32_t num_writes = file_resource_manager.GetNumWritesInProgress(fileid_candidate);
      int fd = 0;
      if (num_writes > 0) {
        uint64_t epoch = file_resource_manager.GetEpochLastActivity(fileid_candidate);
        if (epoch > epoch_now - db_options_.storage__inactivity_timeout) {
          // The in-progress writes for this file haven't timed out yet, thus it
          // is not stable yet.
          break;
        } else {
          // The file epoch is such that the in-progress writes to the file have
          // timed out. All temporary data is cleared: future incoming writes to
          // this file will fail, and at the next startup, its internal index
          // will be recovered. And this is what we want: we don't want to
          // recover the file now, the recovery process should only run at
          // database startup.
          // TODO-37: cleanup key_to_location and key_to_headersize for all keys
          //          that belong to the file id being cleaned up.
          std::string filepath = GetFilepath(fileid_candidate);
          bool is_file_large = file_resource_manager.IsFileLarge(fileid_candidate);

          // Remove large files that are detected as being inactive
          if (is_file_large) {
            if (std::remove(filepath.c_str()) != 0) {
              log::emerg("GetHighestStableFileId()", "Could not remove large file [%s]", filepath.c_str());
            }
            file_resource_manager.ClearTemporaryDataForFileId(fileid_candidate);
            goto handle_next_file;
          }

          // Write the OffsetArray for this inactive file
          log::trace("HSTableManager::GetHighestStableFileId()", "About to write Offset Array");
          if ((fd = open(filepath.c_str(), O_WRONLY, 0644)) < 0) {
            log::emerg("HSTableManager::GetHighestStableFileId()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
            goto handle_next_file;
          }

          // TODO: factorize this code with FlushOffsetArray()
          uint64_t filesize_before = file_resource_manager.GetFileSize(fileid_candidate);
          if (ftruncate(fd, filesize_before) < 0) {
            log::emerg("HSTableManager::GetHighestStableFileId()", "Error ftruncate(): %s", strerror(errno));
            goto handle_next_file;
          }
          uint64_t size_offarray;
          Status s = WriteOffsetArray(fd, file_resource_manager.GetOffsetArray(fileid_candidate), &size_offarray, filetype_default_, file_resource_manager.HasPaddingInValues(fileid_candidate), true);
          if (!s.IsOK()) {
            log::emerg("HSTableManager::GetHighestStableFileId()", "Error on WriteOffsetArray(): %s", s.ToString().c_str());
            goto handle_next_file;
          }
          uint64_t filesize = file_resource_manager.GetFileSize(fileid_candidate);
          file_resource_manager.SetFileSize(fileid_candidate, filesize + size_offarray);
          file_resource_manager.ClearTemporaryDataForFileId(fileid_candidate);
        }
      }
      fileid_stable = fileid_candidate;
      handle_next_file:
      if (fd != 0) close(fd);
      fileid_candidate += 1;
    }
    return fileid_stable;
  }

  void OpenNewFile() {
    log::trace("HSTableManager::OpenNewFile()", "Opening file (before) [%s]: %u", filepath_.c_str(), GetSequenceFileId());
    IncrementSequenceFileId(1);
    IncrementSequenceTimestamp(1);
    filepath_ = GetFilepath(GetSequenceFileId());
    log::trace("HSTableManager::OpenNewFile()", "Opening file [%s]: %u", filepath_.c_str(), GetSequenceFileId());
    while (true) {
      if ((fd_ = open(filepath_.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
        log::emerg("HSTableManager::OpenNewFile()", "Could not open file [%s]: %s", filepath_.c_str(), strerror(errno));
        wait_until_can_open_new_files_ = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(db_options_.internal__open_file_retry_delay));
        continue;
      }
      wait_until_can_open_new_files_ = false;
      break;
    }

    has_file_ = true;
    fileid_ = GetSequenceFileId();
    timestamp_ = GetSequenceTimestamp();

    // Reserving space for header
    offset_start_ = 0;
    offset_end_ = db_options_.internal__hstable_header_size;

    // Filling in default header
    struct HSTableHeader hstheader;
    hstheader.filetype  = filetype_default_;
    hstheader.timestamp = timestamp_;
    HSTableHeader::EncodeTo(&hstheader, &db_options_, buffer_raw_);
  }

  bool CanOpenNewFiles() {
    return !wait_until_can_open_new_files_;
  }

  void CloseCurrentFile() {
    if (!has_file_) return;
    log::trace("HSTableManager::CloseCurrentFile()", "ENTER - fileid_:%d", fileid_);

    // The offarray should only be written if there are no more incoming writes to
    // the current file. If there are still writes in progress, the offarray should not
    // be written so that the next database start-up can trigger a recovery process.
    // Same goes with files that had in-progress writes but timed out: their
    // offarray will not be written so that they will be recovered at start-up.
    FlushOffsetArray();

    close(fd_);
    buffer_has_items_ = false;
    has_file_ = false;
  }

  uint32_t FlushCurrentFile(int force_new_file=0, uint64_t padding=0) {
    if (!has_file_) return 0;
    uint32_t fileid_out = fileid_;
    log::trace("HSTableManager::FlushCurrentFile()", "ENTER - fileid_:%d, has_file_:%d, buffer_has_items_:%d", fileid_, has_file_, buffer_has_items_);
    if (has_file_ && buffer_has_items_) {
      log::trace("HSTableManager::FlushCurrentFile()", "has_files && buffer_has_items_ - fileid_:%d", fileid_);
      if (write(fd_, buffer_raw_ + offset_start_, offset_end_ - offset_start_) < 0) {
        log::emerg("HSTableManager::FlushCurrentFile()", "Error write(): %s", strerror(errno));
        return 0;
      }
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      offset_start_ = offset_end_;
      buffer_has_items_ = false;
      log::trace("HSTableManager::FlushCurrentFile()", "items written - offset_end_:%d | size_block_:%d | force_new_file:%d", offset_end_, size_block_, force_new_file);
    }

    if (padding) {
      offset_end_ += padding;
      offset_start_ = offset_end_;
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      if (ftruncate(fd_, offset_end_) < 0) {
        log::emerg("HSTableManager::FlushCurrentFile()", "Error ftruncate(): %s", strerror(errno));
        return 0;
      }
      if (lseek(fd_, 0, SEEK_END) < 0) {
        log::emerg("HSTableManager::FlushCurrentFile()", "Error lseek(): %s", strerror(errno));
        return 0;
      }
    }

    if (has_sync_option_) {
      has_sync_option_ = false;
      if (FileUtil::sync_file(fd_) < 0) {
        log::emerg("HSTableManager::FlushCurrentFile()", "Error sync_file(): %s", strerror(errno));
      }
    }

    if (offset_end_ >= size_block_ || (force_new_file && offset_end_ > db_options_.internal__hstable_header_size)) {
      log::trace("HSTableManager::FlushCurrentFile()", "file renewed - force_new_file:%d", force_new_file);
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      CloseCurrentFile();
      //OpenNewFile();
    } else {
      //fileid_out = fileid_out - 1;
    }
    log::trace("HSTableManager::FlushCurrentFile()", "done!");
    return fileid_out;
  }


  Status FlushOffsetArray() {
    if (!has_file_) return Status::OK();
    uint32_t num = file_resource_manager.GetNumWritesInProgress(fileid_);
    log::trace("HSTableManager::FlushOffsetArray()", "ENTER - fileid_:%d - num_writes_in_progress:%u", fileid_, num);
    if (file_resource_manager.GetNumWritesInProgress(fileid_) == 0) {
      uint64_t size_offarray;
      file_resource_manager.SetFileSize(fileid_, offset_end_);
      if (ftruncate(fd_, offset_end_) < 0) {
        return Status::IOError("HSTableManager::FlushOffsetArray()", strerror(errno));
      }
      Status s = WriteOffsetArray(fd_, file_resource_manager.GetOffsetArray(fileid_), &size_offarray, filetype_default_, file_resource_manager.HasPaddingInValues(fileid_), false);
      uint64_t filesize = file_resource_manager.GetFileSize(fileid_);
      file_resource_manager.SetFileSize(fileid_, filesize + size_offarray);
      return s;
    }
    return Status::OK();
  }


  Status WriteOffsetArray(int fd,
                          const std::vector< std::pair<uint64_t, uint32_t> >& offarray_current,
                          uint64_t* size_out,
                          FileType filetype,
                          bool has_padding_in_values,
                          bool has_invalid_entries) {
    uint64_t offset = 0;
    struct OffsetArrayRow row;
    for (auto& p: offarray_current) {
      row.hashed_key = p.first;
      row.offset_entry = p.second;
      uint32_t length = OffsetArrayRow::EncodeTo(&row, buffer_index_ + offset);
      offset += length;
      log::trace("HSTableManager::WriteOffsetArray()", "hashed_key:[0x%" PRIx64 "] offset:[0x%08x]", p.first, p.second);
    }

    int64_t position = lseek(fd, 0, SEEK_END);
    if (position < 0) {
      return Status::IOError("HSTableManager::WriteOffsetArray()", strerror(errno));
    }
    log::trace("HSTableManager::WriteOffsetArray()", "file position:[%" PRIu64 "]", position);

    struct HSTableFooter footer;
    footer.filetype = filetype;
    footer.offset_indexes = position;
    footer.num_entries = offarray_current.size();
    footer.magic_number = get_magic_number();
    if (has_padding_in_values) footer.SetFlagHasPaddingInValues();
    if (has_invalid_entries) footer.SetFlagHasInvalidEntries();
    uint32_t length = HSTableFooter::EncodeTo(&footer, buffer_index_ + offset);
    offset += length;

    uint32_t crc32 = crc32c::Value(buffer_index_, offset - 4);
    EncodeFixed32(buffer_index_ + offset - 4, crc32);

    if (write(fd, buffer_index_, offset) < 0) {
      log::trace("HSTableManager::WriteOffsetArray()", "Error write(): %s", strerror(errno));
    }

    // ftruncate() is necessary in case the file system space for the file was pre-allocated 
    if (ftruncate(fd, position + offset) < 0) {
      return Status::IOError("HSTableManager::WriteOffsetArray()", strerror(errno));
    }

    *size_out = offset;
    log::trace("HSTableManager::WriteOffsetArray()", "offset_indexes:%u, num_entries:[%lu]", position, offarray_current.size());
    return Status::OK();
  }


  uint64_t WriteFirstChunkLargeOrder(Order& order, uint64_t hashed_key) {
    // If a large order is self-contained, it will still be split into chunks,
    // and therefore the opearations on the first and last chunks will be done
    // as expected. See notes in WriteOrdersAndFlushFile() for more information.

    // TODO-30: large files should be pre-allocated. The problem here is that
    // the streaming interface needs to work over a network, thus the
    // pre-allocation can't block or take too long.
    uint64_t fileid_largefile = IncrementSequenceFileId(1);
    uint64_t timestamp_largefile = IncrementSequenceTimestamp(1);
    std::string filepath = GetFilepath(fileid_largefile);
    log::trace("HSTableManager::WriteFirstChunkLargeOrder()", "filepath:[%s] key:[%s] tid:[0x%08" PRIx64 "]", filepath.c_str(), order.key.ToString().c_str(), order.tid);
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return 0;
    }

    // Write hstable header
    char buffer[db_options_.internal__hstable_header_size];
    struct HSTableHeader hstheader;
    hstheader.filetype  = kCompactedLargeType;
    hstheader.timestamp = timestamp_largefile;
    HSTableHeader::EncodeTo(&hstheader, &db_options_, buffer);
    if (write(fd, buffer, db_options_.internal__hstable_header_size) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error write(): %s", strerror(errno));
      return 0;
    }

    // Write entry header
    struct EntryHeader entry_header;
    entry_header.SetTypePut();
    entry_header.SetEntryFull();
    entry_header.size_key = order.key.size();
    entry_header.size_value = order.size_value;
    entry_header.size_value_compressed = order.size_value_compressed;
    entry_header.size_padding = 0;
    entry_header.hash = hashed_key;
    entry_header.crc32 = 0;
    entry_header.SetIsUncompacted(false);
    entry_header.SetHasPadding(false);
    uint32_t size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer);
    key_to_headersize[order.tid][order.key.ToString()] = size_header;
    if (write(fd, buffer, size_header) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error write(): %s", strerror(errno));
      return 0;
    }

    // Write key and chunk
    // NOTE: Could also put the key and chunk in the buffer and do a single write
    if (write(fd, order.key.data(), order.key.size()) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error write(): %s", strerror(errno));
      return 0;
    }

    if (write(fd, order.chunk.data(), order.chunk.size()) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error write(): %s", strerror(errno));
      return 0;
    }

    uint64_t filesize = db_options_.internal__hstable_header_size + size_header + order.key.size() + order.size_value;
    if (ftruncate(fd, filesize) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error ftruncate(): %s", strerror(errno));
      return 0;
    }

    if (order.write_options.sync && FileUtil::sync_file(fd) < 0) {
      log::emerg("HSTableManager::WriteFirstChunkLargeOrder()", "Error sync_file(): %s", strerror(errno));
    }

    file_resource_manager.SetFileSize(fileid_largefile, filesize);
    close(fd);
    uint64_t fileid_shifted = fileid_largefile;
    fileid_shifted <<= 32;
    uint64_t location = fileid_shifted | db_options_.internal__hstable_header_size;
    log::trace("HSTableManager::WriteFirstChunkLargeOrder()", "fileid [%d] location: [%" PRIu64 "]", fileid_largefile, location);
    file_resource_manager.SetNumWritesInProgress(fileid_largefile, 1);
    file_resource_manager.AddOffsetArray(fileid_largefile, std::pair<uint64_t, uint32_t>(hashed_key, db_options_.internal__hstable_header_size));
    return location;
  }


  uint64_t WriteMiddleOrLastChunk(Order& order, uint64_t hashed_key, uint64_t location) {
    uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
    uint32_t offset_file = location & 0x00000000FFFFFFFF;
    std::string filepath = GetFilepath(fileid);

    if (fileid != fileid_ && file_resource_manager.GetNumWritesInProgress(fileid) == 0) {
      // This file is not the lastest file, and it has no writes in progress.
      // The file was either closed or the writes timed out, therefore do nothing
      return 0;
    }

    log::trace("HSTableManager::WriteMiddleOrLastChunk()", "key [%s] filepath:[%s] offset_chunk:%" PRIu64, order.key.ToString().c_str(), filepath.c_str(), order.offset_chunk);
    int fd = 0;
    if ((fd = open(filepath.c_str(), O_WRONLY, 0644)) < 0) {
      log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return 0;
    }

    if (key_to_headersize.find(order.tid) == key_to_headersize.end() ||
        key_to_headersize[order.tid].find(order.key.ToString()) == key_to_headersize[order.tid].end()) {
      log::trace("HSTableManager::WriteMiddleOrLastChunk()", "Missing in key_to_headersize[]");
    }

    uint32_t size_header = key_to_headersize[order.tid][order.key.ToString()];

    // Write the chunk
    if (pwrite(fd,
               order.chunk.data(),
               order.chunk.size(),
               offset_file + size_header + order.key.size() + order.offset_chunk) < 0) {
      log::trace("HSTableManager::WriteMiddleOrLastChunk()", "Error pwrite(): %s", strerror(errno));
    }

    // If this is a last chunk, the header is written again to save the right size of compressed value,
    // and the crc32 is saved too
    if (order.IsLastChunk()) {
      log::trace("HSTableManager::WriteMiddleOrLastChunk()", "Write compressed size: [%s] - size:%" PRIu64 ", compressed size:%" PRIu64 " crc32:0x%08" PRIx64, order.key.ToString().c_str(), order.size_value, order.size_value_compressed, order.crc32);
      struct EntryHeader entry_header;
      entry_header.SetTypePut();
      entry_header.SetEntryFull();
      entry_header.size_key = order.key.size();
      entry_header.size_value = order.size_value;
      entry_header.size_value_compressed = order.size_value_compressed;
      entry_header.size_padding = order.IsLarge() ? 0 : EntryHeader::CalculatePaddingSize(order.size_value);
      entry_header.hash = hashed_key;
      if (!order.IsLarge() && entry_header.IsCompressed()) {
        // NOTE: entry_header.IsCompressed() makes no sense since compression is
        // handled at database level, not at entry level. All usages of
        // IsCompressed() should be replaced by a check on the database options.
        entry_header.SetIsUncompacted(true);
        file_resource_manager.SetHasPaddingInValues(fileid_, true);
        entry_header.SetHasPadding(true);
      }
      entry_header.print();

      // Compute the header a first time to get the data serialized
      char buffer[sizeof(struct EntryHeader)*2];
      uint32_t size_header_new = EntryHeader::EncodeTo(db_options_, &entry_header, buffer);

      // Compute the checksum for the header and combine it with the one for the
      // key and value, then recompute the header to save the checksum
      uint32_t crc32_header = crc32c::Value(buffer + 4, size_header_new - 4);
      entry_header.crc32 = crc32c::Combine(crc32_header, order.crc32, entry_header.size_key + entry_header.size_value_used());
      log::trace("HSTableManager::WriteMiddleOrLastChunk()", "CRC32 header: 0x%08" PRIx64, crc32_header);
      entry_header.print();
      size_header_new = EntryHeader::EncodeTo(db_options_, &entry_header, buffer);
      if (size_header_new != size_header) {
        log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error of encoding: the initial header had a size of %u, and it is now %u. The entry is now corrupted.", size_header, size_header_new);
        return 0;
      }

      if (pwrite(fd, buffer, size_header, offset_file) < 0) {
        log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error pwrite(): %s", strerror(errno));
        return 0;
      }
 
      if (order.IsLarge() && entry_header.IsCompressed()) {
        uint64_t filesize = db_options_.internal__hstable_header_size + size_header + order.key.size() + order.size_value_compressed;
        file_resource_manager.SetFileSize(fileid, filesize);
        if (ftruncate(fd, filesize) < 0) {
          log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error ftruncate(): %s", strerror(errno));
          return 0;
        }
      }

      uint32_t num_writes_in_progress = file_resource_manager.SetNumWritesInProgress(fileid, -1);
      if (fileid != fileid_ && num_writes_in_progress == 0) {
        // TODO: factorize this code with FlushOffsetArray()
        log::trace("HSTableManager::WriteMiddleOrLastChunk()", "About to write Offset Array");
        uint64_t size_offarray;
        FileType filetype = order.IsLarge() ? kCompactedLargeType : filetype_default_;
        uint64_t filesize_before = file_resource_manager.GetFileSize(fileid);
        if (ftruncate(fd, filesize_before) < 0) {
          log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error ftruncate(): %s", strerror(errno));
          return 0;
        }
        Status s = WriteOffsetArray(fd, file_resource_manager.GetOffsetArray(fileid), &size_offarray, filetype, file_resource_manager.HasPaddingInValues(fileid), false);
        if (!s.IsOK()) {
          log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error on WriteOffsetArray(): %s", s.ToString().c_str());
          return 0;
        }
        uint64_t filesize = file_resource_manager.GetFileSize(fileid);
        file_resource_manager.SetFileSize(fileid, filesize + size_offarray);
        if (order.IsLarge()) file_resource_manager.SetFileLarge(fileid);
        file_resource_manager.ClearTemporaryDataForFileId(fileid);
      }
    }

    if (order.write_options.sync && FileUtil::sync_file(fd) < 0) {
      log::emerg("HSTableManager::WriteMiddleOrLastChunk()", "Error sync_file(): %s", strerror(errno));
    }

    close(fd);
    log::trace("HSTableManager::WriteMiddleOrLastChunk()", "all good");
    return location;
  }


  uint64_t WriteFirstChunkOrSmallOrder(Order& order, uint64_t hashed_key) {

    if (order.write_options.sync) {
      has_sync_option_ = true;
    }

    uint64_t location_out = 0;
    struct EntryHeader entry_header;

    if (order.type == OrderType::Put) {
      entry_header.SetTypePut();
      entry_header.SetEntryFull();
      entry_header.size_key = order.key.size();
      entry_header.size_value = order.size_value;
      entry_header.size_value_compressed = order.size_value_compressed;
      entry_header.hash = hashed_key;
      entry_header.crc32 = order.crc32;
      if (order.IsSelfContained()) {
        entry_header.SetIsUncompacted(false);
        entry_header.SetHasPadding(false);
        entry_header.size_padding = 0;
      } else {
        entry_header.SetIsUncompacted(true);
        file_resource_manager.SetHasPaddingInValues(fileid_, true);
        entry_header.SetHasPadding(true);
        entry_header.size_padding = EntryHeader::CalculatePaddingSize(order.size_value);
        // TODO: check that the has_padding_in_values field in fields is used during compaction
      }
      uint32_t size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer_raw_ + offset_end_);

      if (order.IsSelfContained()) {
        // Compute the checksum for the header and combine it with the one for the
        // key and value, then recompute the header to save the checksum
        uint32_t crc32_header = crc32c::Value(buffer_raw_ + offset_end_ + 4, size_header - 4);
        entry_header.crc32 = crc32c::Combine(crc32_header, order.crc32, entry_header.size_key + entry_header.size_value_used());
        size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer_raw_ + offset_end_);
        log::trace("HSTableManager::WriteFirstChunkOrSmallOrder()", "IsSelfContained():true - crc32 [0x%08x]", entry_header.crc32);
      }

      memcpy(buffer_raw_ + offset_end_ + size_header, order.key.data(), order.key.size());
      memcpy(buffer_raw_ + offset_end_ + size_header + order.key.size(), order.chunk.data(), order.chunk.size());

      //map_index[order.key] = fileid_ | offset_end_;
      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      file_resource_manager.AddOffsetArray(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      offset_end_ += size_header + order.key.size() + order.chunk.size();

      if (!order.IsSelfContained()) {
        key_to_headersize[order.tid][order.key.ToString()] = size_header;
        log::trace("HSTableManager::WriteFirstChunkOrSmallOrder()", "BEFORE fileid_ %u", fileid_);
        file_resource_manager.SetNumWritesInProgress(fileid_, 1);
        FlushCurrentFile(0, entry_header.size_value_offset() - order.chunk.size());
        // NOTE: A better way to do it would be to copy things into the buffer, and
        // then for the other chunks, either copy in the buffer if the position
        // to write is >= offset_end_, or do a pwrite() if the position is <
        // offset_end_
        // NOTE: might be better to lseek() instead of doing a large write
        // NOTE: No longer necessary to do the lseek() here, as I'm doing it in
        // the FlushCurrentFile()
        //offset_end_ += order.size_value - order.size_chunk;
        //FlushCurrentFile();
        //ftruncate(fd_, offset_end_);
        //lseek(fd_, 0, SEEK_END);
        log::trace("HSTableManager::WriteFirstChunkOrSmallOrder()", "AFTER fileid_ %u", fileid_);
      }
      log::trace("HSTableManager::WriteFirstChunkOrSmallOrder()", "Put [%s]", order.key.ToString().c_str());
    } else { // order.type == OrderType::Delete
      log::trace("HSTableManager::WriteFirstChunkOrSmallOrder()", "Delete [%s]", order.key.ToString().c_str());
      entry_header.SetTypeDelete();
      entry_header.SetEntryFull();
      entry_header.size_key = order.key.size();
      entry_header.size_value = 0;
      entry_header.size_value_compressed = 0;
      entry_header.crc32 = 0;
      uint32_t size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer_raw_ + offset_end_);
      memcpy(buffer_raw_ + offset_end_ + size_header, order.key.data(), order.key.size());

      uint64_t fileid_shifted = fileid_;
      fileid_shifted <<= 32;
      location_out = fileid_shifted | offset_end_;
      file_resource_manager.AddOffsetArray(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
      offset_end_ += size_header + order.key.size();
    }
    return location_out;
  }

  void WriteOrdersAndFlushFile(std::vector<Order>& orders, std::multimap<uint64_t, uint64_t>& map_index_out) {
    for (auto& order: orders) {

      if (offset_end_ > size_block_) {
        log::trace("HSTableManager::WriteOrdersAndFlushFile()", "About to flush - offset_end_: %" PRIu64 " | size_key: %d | size_value: %d | size_block_: %" PRIu64, offset_end_, order.key.size(), order.size_value, size_block_);
        FlushCurrentFile(true, 0);
      }

      if (!has_file_) OpenNewFile();

      uint64_t hashed_key = hash_->HashFunction(order.key.data(), order.key.size());
      // TODO-13: if the item is self-contained (unique chunk), then no need to
      //       have size_value space, size_value_compressed is enough.

      // TODO-12: If the db is embedded, then all order are self contained,
      //       independently of their sizes. Would the compression and CRC32 still
      //       work? Would storing the data (i.e. choosing between the different
      //       storing functions) still work?

      // There are three categories of entries:
      //  - Small entries:  sizes within [0, server.size_buffer_recv)
      //  - Medium entries: sizes within [server.size_buffer_recv, hstable.maximum_size)
      //  - Large entries:  sizes greater than hstable.maximum_size
      //
      // When using the storage engine through a network interface, medium and
      // large entries are split into chunks of size at most server.size_buffer_recv,
      // making them "multi-chunk" entries.
      // Small entries do not need to be split, and are therefore "self-contained".
      // Chunks are held into "orders", which hold extra metadata needed
      // for various steps of the storage process.
      // There are three types of chunks, based on their positions in the data
      // stream: first chunk, middle chunk, and last chunk. Different operations
      // need to be completed on an order depending on the type of chunk it
      // contains.
      //
      // When using the storage engine embedded in another program, orders can be
      // on any size, and because it is embedded, the data can be sent as is to
      // the storage engine, potentially in a very large buffer, larger than
      // the size of server.size_buffer_recv contrained when on a network. Because the
      // logic in the storage engine expects first and last chunks, a large
      // order that is at the same time a first *and* a last chunk could cause
      // an issue: the order could be treated only as a first chunk,
      // and the operations triggered by the arrival of the last chunk
      // may not be done. To solve that problem, and because compression
      // and hash functions take input of limited sizes anyway, the constant
      // 'maximum_chunk_size' has been introduced. As part of the
      // KingDB::PutChunk() method, the sizes of incoming orders are checked,
      // and if they are larger than 'maximum_chunk_size', they are split
      // into smaller chunks. This is done in such a way that any
      // self-contained large entry would be split, therefore guaranteeing
      // that that the operations done by both the first and last chunks
      // are triggered.
      //
      // For performance reasons, the small and medium entries incoming during
      // the same time period are grouped together in a buffer and written
      // at once to a "regular" HSTable. Large entries are written to their own
      // HSTable, referred to as "large" HSTable.
 

      // 1. The order is the first chunk of a large entry, so we create a
      //    large HSTable and write the first chunk in there
      uint64_t location = 0;
      if (order.IsLarge() && order.IsFirstChunk()) {
        // TODO-11: shouldn't this be testing size_value_compressed as well? -- yes, only if the order
        // is a full entry by itself (will happen when the kvstore will be embedded and not accessed
        // through the network), otherwise we don't know yet what the total compressed size will be.
        location = WriteFirstChunkLargeOrder(order, hashed_key);

      // 2. The order is a middle or last chunk, so we open the HSTable,
      //    pwrite() the chunk, and close the HSTable
      } else if (order.IsMiddleOrLastChunk()) {
        //  TODO-11: replace the tests on compression "order.size_value_compressed ..." by a real test on a flag or a boolean
        //  TODO-11: replace the use of size_value or size_value_compressed by a unique size() which would already return the right value
        if (key_to_location.find(order.tid) == key_to_location.end()) {
          location = 0;
        } else {
          location = key_to_location[order.tid][order.key.ToString()];
        }
        if (location != 0) {
          WriteMiddleOrLastChunk(order, hashed_key, location);
        } else {
          log::emerg("HSTableManager", "Avoided catastrophic location error (in case 2) key:[%s] tid:[0x%08" PRIx64 "]", order.key.ToString().c_str(), order.tid); 
          for (auto& p: key_to_location[order.tid]) {
            log::emerg("HSTableManager", "key:%s value:%" PRIu64, p.first.c_str(), p.second);
          }
        }

      // 3. The order is a self-contained small chunk, or a first chunk
      //    for a medium entry, thus it is added to the current buffer and
      //    is written to the latest on-going HSTable
      } else {
        buffer_has_items_ = true;
        location = WriteFirstChunkOrSmallOrder(order, hashed_key);
      }

      // Traces
      int caseid = 0;
      if (order.IsLarge() && order.IsFirstChunk()) { caseid = 1; }
      else if (order.IsMiddleOrLastChunk()) { caseid = 2; }
      else { caseid = 3; }
      log::trace("HSTableManager::WriteOrdersAndFlushFile()",
                "%d. key: [%s] size_chunk:%" PRIu64 " offset_chunk: %" PRIu64,
                caseid, order.key.ToString().c_str(), order.chunk.size(), order.offset_chunk);


      // If the order is self-contained or a last chunk,
      // add his location to the output map_index_out[]
      if (order.IsSelfContained() || order.IsLastChunk()) {
        log::trace("HSTableManager::WriteOrdersAndFlushFile()", "END OF ORDER key: [%s] size_chunk:%" PRIu64 " offset_chunk: %" PRIu64 " location:%" PRIu64, order.key.ToString().c_str(), order.chunk.size(), order.offset_chunk, location);
        if (location != 0) {
          map_index_out.insert(std::pair<uint64_t, uint64_t>(hashed_key, location));
        } else {
          log::emerg("HSTableManager", "Avoided catastrophic location error (post-processing last chunk)"); 
        }
        if (key_to_location.find(order.tid) != key_to_location.end()) {
          key_to_location[order.tid].erase(order.key.ToString());
        }
        if (key_to_headersize.find(order.tid) != key_to_headersize.end()) {
          key_to_headersize[order.tid].erase(order.key.ToString());
        }
      // Else, if the order is not self-contained and is a first chunk,
      // the location is saved in key_to_location[]
      } else if (order.IsFirstChunk()) {
        if (location != 0 && order.type != OrderType::Delete) {
          key_to_location[order.tid][order.key.ToString()] = location;
          log::trace("HSTableManager", "location saved: [%" PRIu64 "]", location); 
        } else {
          log::trace("HSTableManager", "Avoided catastrophic location error (post-processing first chunk)"); 
        }
      }
    }
    log::trace("HSTableManager::WriteOrdersAndFlushFile()", "end flush");
    FlushCurrentFile(0, 0);
  }


  static Status LoadDatabaseOptionsFromHSTables(std::string& dbname,
                                                DatabaseOptions* db_options_out,
                                                std::string& prefix_compaction) {
    // Careful here, code duplication: all of the directory walking and
    // file selection was taken from LoadDatabase()
  
    log::trace("HSTableManager::LoadDatabaseOptionsFromHSTables()", "Start");
    char filepath[FileUtil::maximum_path_size()];
    DIR *directory;
    struct dirent *entry;
    struct stat info;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }

    bool found_valid_db_options = false;
    while ((entry = readdir(directory)) != NULL) {
      if (strcmp(entry->d_name, DatabaseOptions::GetFilename().c_str()) == 0) continue;
      if (strcmp(entry->d_name, prefix_compaction.c_str()) == 0) continue;
      int ret = snprintf(filepath, FileUtil::maximum_path_size(), "%s/%s", dbname.c_str(), entry->d_name);
      if (ret < 0 || ret >= FileUtil::maximum_path_size()) {
        log::emerg("HSTableManager::LoadDatabaseOptionsFromHSTables()",
                  "Filepath buffer is too small, could not build the filepath string for file [%s]", entry->d_name); 
        continue;
      }
      if (stat(filepath, &info) != 0 || !(info.st_mode & S_IFREG)) continue;
      // Yes, using the default internal__hstable_header_size value from the
      // object this method is meant to return.
      if (info.st_size <= db_options_out->internal__hstable_header_size) {
        log::trace("HSTableManager::LoadDatabaseOptionsFromHSTables()",
                  "file: [%s] only has a header or less, skipping\n", entry->d_name);
        continue;
      }

      Mmap mmap(filepath, info.st_size);
      if (!mmap.is_valid()) return Status::IOError("Mmap constructor failed");
      struct HSTableHeader hstheader;
      struct DatabaseOptions db_options;
      Status s = HSTableHeader::DecodeFrom(mmap.datafile(), mmap.filesize(), &hstheader, &db_options);
      if (s.IsOK()) {
        *db_options_out = db_options;
        found_valid_db_options = true;
        break;
      } else {
        log::trace("HSTableManager::LoadDatabaseOptionsFromHSTables()",
                   "file: [%s] has an invalid header, skipping\n", entry->d_name);
      }
    }
    if (found_valid_db_options) {
      return Status::OK();
    } else {
      return Status::IOError("Could not find any HSTable with a valid database option backup.");
    }
  }


  Status LoadDatabase(std::string& dbname,
                      std::multimap<uint64_t, uint64_t>& index_se,
                      std::set<uint32_t>* fileids_ignore=nullptr,
                      uint32_t fileid_end=0,
                      std::vector<uint32_t>* fileids_iterator=nullptr) {
    Status s;
    struct stat info;

    if (!is_read_only_) {
      if (   stat(dirpath_locks_.c_str(), &info) != 0
          && mkdir(dirpath_locks_.c_str(), 0755) < 0) {
        return Status::IOError("Could not create lock directory", strerror(errno));
      }

      /*
      if(!(info.st_mode & S_IFDIR)) {
        return Status::IOError("A file with same name as the lock directory already exists and is not a directory. Delete or rename this file to continue.", dirpath_locks_.c_str());
      }
      */

      s = FileUtil::remove_files_with_prefix(dbname_.c_str(), prefix_compaction_);
      if (!s.IsOK()) return Status::IOError("Could not clean up previous compaction");

      s = DeleteAllLockedFiles(dbname_);
      if (!s.IsOK()) return Status::IOError("Could not clean up snapshots");

      s = FileUtil::remove_files_with_prefix(dirpath_locks_.c_str(), "");
      if (!s.IsOK()) return Status::IOError("Could not clean up locks");
    }

    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dbname.c_str())) == NULL) {
      return Status::IOError("Could not open database directory", dbname.c_str());
    }

    // Sort the fileids by <timestamp, fileid>, so that puts and removes can be
    // applied in the right order.
    // Indeed, imagine that we have files with ids from 1 to 100, and a
    // compaction process operating on files 1 through 50. The files 1-50 are
    // going to be compacted and the result of this compaction written
    // to ids 101 and above, which means that even though the entries in
    // files 101 and above are older than the entries in files 51-100, they are
    // in files with greater ids. Thus, the file ids cannot be used as a safe
    // way to order the entries in a set of files, and we need to have a sequence id
    // which will allow all other processes to know what is the order of
    // the entries in a set of files, which is why we have a 'timestamp' in each
    // file. As a consequence, the sequence id is the concatenation of
    // the 'timestamp' and the 'fileid'.
    // As the compaction process will always include at least one uncompacted
    // file, the maximum timestamp is garanteed to be always increasing and no
    // overlapping will occur.
    std::map<std::string, uint32_t> timestamp_fileid_to_fileid;
    char filepath[FileUtil::maximum_path_size()];
    char buffer_key[64]; // buffer used to order HSTables when loading a database,
                         // shouldn't need more than 33 bytes, but rounded up
    uint32_t fileid_max = 0;
    uint64_t timestamp_max = 0;
    uint32_t fileid = 0;
    while ((entry = readdir(directory)) != NULL) {
      if (strcmp(entry->d_name, DatabaseOptions::GetFilename().c_str()) == 0) continue;
      if (strcmp(entry->d_name, prefix_compaction_.c_str()) == 0) continue;
      int ret = snprintf(filepath, FileUtil::maximum_path_size(), "%s/%s", dbname.c_str(), entry->d_name);
      if (ret < 0 || ret >= FileUtil::maximum_path_size()) {
        log::emerg("HSTableManager::LoadDatabase()",
                  "Filepath buffer is too small, could not build the filepath string for file [%s]", entry->d_name); 
        continue;
      }
      if (stat(filepath, &info) != 0 || !(info.st_mode & S_IFREG)) continue;
      fileid = HSTableManager::hex_to_num(entry->d_name);
      if (   fileids_ignore != nullptr
          && fileids_ignore->find(fileid) != fileids_ignore->end()) {
        log::trace("HSTableManager::LoadDatabase()",
                  "Skipping file in fileids_ignore:: [%s] [%lld] [%u]\n",
                  entry->d_name, info.st_size, fileid);
        continue;
      }
      if (fileid_end != 0 && fileid > fileid_end) {
        log::trace("HSTableManager::LoadDatabase()",
                  "Skipping file with id larger than fileid_end (%u): [%s] [%lld] [%u]\n",
                  fileid, entry->d_name, info.st_size, fileid);
        continue;
      }
      log::trace("HSTableManager::LoadDatabase()",
                "file: [%s] [%lld] [%u]\n", entry->d_name, info.st_size, fileid);
      if (info.st_size <= db_options_.internal__hstable_header_size) {
        log::trace("HSTableManager::LoadDatabase()",
                  "file: [%s] only has a header or less, skipping\n", entry->d_name);
        continue;
      }

      Mmap mmap(filepath, info.st_size);
      if (!mmap.is_valid()) return Status::IOError("Mmap constructor failed");
      struct HSTableHeader hstheader;
      Status s = HSTableHeader::DecodeFrom(mmap.datafile(), mmap.filesize(), &hstheader);
      if (!s.IsOK()) {
        log::trace("HSTableManager::LoadDatabase()",
                  "file: [%s] has an invalid header, skipping\n", entry->d_name);
        continue;
      }

      sprintf(buffer_key, "%016" PRIx64 "-%016x", hstheader.timestamp, fileid);
      std::string key(buffer_key);
      timestamp_fileid_to_fileid[key] = fileid;
      fileid_max = std::max(fileid_max, fileid);
      timestamp_max = std::max(timestamp_max, hstheader.timestamp);
    }

    for (auto& p: timestamp_fileid_to_fileid) {
      uint32_t fileid = p.second;
      if (fileids_iterator != nullptr) fileids_iterator->push_back(fileid);
      std::string filepath = GetFilepath(fileid);
      log::trace("HSTableManager::LoadDatabase()", "Loading file:[%s] with key:[%s]", filepath.c_str(), p.first.c_str());
      if (stat(filepath.c_str(), &info) != 0) continue;
      Mmap mmap(filepath.c_str(), info.st_size);
      if (!mmap.is_valid()) return Status::IOError("Mmap constructor failed");
      uint64_t filesize;
      bool is_file_large, is_file_compacted;
      s = LoadFile(mmap, fileid, index_se, &filesize, &is_file_large, &is_file_compacted);
      if (s.IsOK()) { 
        file_resource_manager.SetFileSize(fileid, filesize);
        if (is_file_large) file_resource_manager.SetFileLarge(fileid);
        if (is_file_compacted) file_resource_manager.SetFileCompacted(fileid);
      } else if (!s.IsOK() && !is_read_only_) {
        log::warn("HSTableManager::LoadDatabase()", "Could not load index in file [%s], entering recovery mode", filepath.c_str());
        s = RecoverFile(mmap, fileid, index_se);
        if (!s.IsOK()) {
          log::warn("HSTableManager::LoadDatabase()", "Recovery failed for file [%s]", filepath.c_str());
          mmap.Close();
          if (std::remove(filepath.c_str()) != 0) {
            log::emerg("HSTableManager::LoadDatabase()", "Could not remove file [%s]", filepath.c_str());
          }
        }
      }
    }
    if (fileid_max > 0) {
      SetSequenceFileId(fileid_max);
      SetSequenceTimestamp(timestamp_max);
    }
    closedir(directory);
    return Status::OK();
  }

  static Status LoadFile(Mmap& mmap,
                  uint32_t fileid,
                  std::multimap<uint64_t, uint64_t>& index_se,
                  uint64_t *filesize_out=nullptr,
                  bool *is_file_large_out=nullptr,
                  bool *is_file_compacted_out=nullptr) {
    log::trace("LoadFile()", "Loading [%s] of size:%u, sizeof(HSTableFooter):%u", mmap.filepath(), mmap.filesize(), HSTableFooter::GetFixedSize());

    struct HSTableFooter footer;
    Status s;
    s = HSTableFooter::DecodeFrom(mmap.datafile() + mmap.filesize() - HSTableFooter::GetFixedSize(),
                                  HSTableFooter::GetFixedSize(),
                                  &footer);
    if (!s.IsOK()) return s;
    if (footer.magic_number != HSTableManager::get_magic_number()) {
      log::trace("LoadFile()", "Skipping [%s] - magic_number:[%" PRIu64 "/%" PRIu64 "]", mmap.filepath(), footer.magic_number, get_magic_number());
      return Status::IOError("Invalid footer");
    }
    
    uint32_t crc32_computed = crc32c::Value(mmap.datafile() + footer.offset_indexes, mmap.filesize() - footer.offset_indexes - 4);
    if (crc32_computed != footer.crc32) {
      log::trace("LoadFile()", "Skipping [%s] - Invalid CRC32:[%08x/%08x]", mmap.filepath(), footer.crc32, crc32_computed);
      return Status::IOError("Invalid footer");
    }
    
    log::trace("LoadFile()", "Footer OK");
    // The file has a clean footer, load all the offsets in the index
    uint64_t offset_index = footer.offset_indexes;
    struct OffsetArrayRow row;
    for (auto i = 0; i < footer.num_entries; i++) {
      uint32_t length_row = 0;
      s = OffsetArrayRow::DecodeFrom(mmap.datafile() + offset_index,
                                     mmap.filesize() - offset_index,
                                     &row,
                                     &length_row);
      if (!s.IsOK()) return s;
      uint64_t fileid_shifted = fileid;
      fileid_shifted <<= 32;
      index_se.insert(std::pair<uint64_t, uint64_t>(row.hashed_key, fileid_shifted | row.offset_entry));
      log::trace("LoadFile()",
                "Add item to index -- hashed_key:[0x%" PRIx64 "] offset:[%u] -- offset_index:[%" PRIu64 "]",
                row.hashed_key, row.offset_entry, offset_index);
      offset_index += length_row;
    }
    if (filesize_out) *filesize_out = mmap.filesize();
    if (is_file_large_out) *is_file_large_out = footer.IsTypeLarge() ? true : false;
    if (is_file_compacted_out) *is_file_compacted_out = footer.IsTypeCompacted() ? true : false;
    log::trace("LoadFile()", "Loaded [%s] num_entries:[%" PRIu64 "]", mmap.filepath(), footer.num_entries);

    return Status::OK();
  }

  Status RecoverFile(Mmap& mmap,
                     uint32_t fileid,
                     std::multimap<uint64_t, uint64_t>& index_se) {
    uint32_t offset = db_options_.internal__hstable_header_size;
    std::vector< std::pair<uint64_t, uint32_t> > offarray_current;
    bool has_padding_in_values = false;
    bool has_invalid_entries   = false;

    struct HSTableHeader hstheader;
    Status s = HSTableHeader::DecodeFrom(mmap.datafile(), mmap.filesize(), &hstheader);
    // 1. If the file is a large file, just discard it
    if (!s.IsOK() || hstheader.IsTypeLarge()) {
      return Status::IOError("Could not recover file");
    }

    // 2. If the file is a hstable, go over all its entries and verify each one of them
    while (true) {
      struct EntryHeader entry_header;
      uint32_t size_header;
      Status s = EntryHeader::DecodeFrom(db_options_, mmap.datafile() + offset, mmap.filesize() - offset, &entry_header, &size_header);
      if (   !s.IsOK()
          || !entry_header.AreSizesValid(offset, mmap.filesize())) {
        // End of file during recovery, thus breaking out of the while-loop
        break;
      }

      // NOTE: The checksum is verified only for uncompacted files, because this
      // is when an entry can be invalid due to transfer or write issues.
      // For compacted files and during the compaction process, it does not
      // matter whether or not the entry is valid. The user will know that
      // an entry is invalid after doing a Get(), and that is his choice to do a
      // Delete() if he wants to delete the entry. Keep in mind though that if
      // the checksum is wrong, it's possible for the hashedkey to be
      // erroneous, in which case the only way to find and remove invalid
      // entries is to iterate over whole database, and do Delete() commands
      // for the entries with invalid checksums.
      bool do_crc32_verification = entry_header.IsUncompacted() ? true : false;
      bool is_crc32_valid = true;
      if (do_crc32_verification) {
        crc32_.ResetThreadLocalStorage();
        crc32_.stream(mmap.datafile() + offset + 4, size_header + entry_header.size_key + entry_header.size_value_used() - 4);
        is_crc32_valid = (entry_header.crc32 == crc32_.get());
      }
      if (!do_crc32_verification || is_crc32_valid) {
        // Valid content, add to index
        offarray_current.push_back(std::pair<uint64_t, uint32_t>(entry_header.hash, offset));
        uint64_t fileid_shifted = fileid;
        fileid_shifted <<= 32;
        index_se.insert(std::pair<uint64_t, uint64_t>(entry_header.hash, fileid_shifted | offset));
      } else {
        has_invalid_entries = true; 
      }

      if (entry_header.HasPadding()) has_padding_in_values = true;
      offset += size_header + entry_header.size_key + entry_header.size_value_offset();
      log::trace("HSTableManager::RecoverFile",
                 "Scanned hash [%" PRIu64 "], next offset [%" PRIu64 "] - CRC32:%s stored=0x%08x computed=0x%08x",
                 entry_header.hash, offset, do_crc32_verification ? (is_crc32_valid?"OK":"ERROR") : "UNKNOWN", entry_header.crc32, crc32_.get());
    }

    // 3. Write a new index at the end of the file with whatever entries could be save
    if (offset > db_options_.internal__hstable_header_size) {
      mmap.Close();
      int fd;
      if ((fd = open(mmap.filepath(), O_WRONLY, 0644)) < 0) {
        log::emerg("HSTableManager::RecoverFile()", "Could not open file [%s]: %s", mmap.filepath(), strerror(errno));
        return Status::IOError("Could not open file for recovery", mmap.filepath());
      }
      if (ftruncate(fd, offset) < 0) {
        return Status::IOError("HSTableManager::RecoverFile()", strerror(errno));
      }
      uint64_t size_offarray;
      Status s = WriteOffsetArray(fd, offarray_current, &size_offarray, hstheader.GetFileType(), has_padding_in_values, has_invalid_entries);
      if (!s.IsOK()) return s;
      file_resource_manager.SetFileSize(fileid, mmap.filesize() + size_offarray);
      close(fd);
    } else {
      return Status::IOError("Could not recover file");
    }

    return Status::OK();
  }


  Status DeleteAllLockedFiles(std::string& dbname) {
    std::set<uint32_t> fileids;
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dirpath_locks_.c_str())) == NULL) {
      return Status::IOError("Could not open lock directory", dirpath_locks_.c_str());
    }

    uint32_t fileid = 0;
    while ((entry = readdir(directory)) != NULL) {
      if (strncmp(entry->d_name, ".", 1) == 0) continue;
      fileid = HSTableManager::hex_to_num(entry->d_name);
      fileids.insert(fileid);
    }

    closedir(directory);

    for (auto& fileid: fileids) {
      if (std::remove(GetFilepath(fileid).c_str()) != 0) {
        log::emerg("DeleteAllLockedFiles()", "Could not remove data file [%s]", GetFilepath(fileid).c_str());
      }
    }

    return Status::OK();
  }


  uint64_t static get_magic_number() { return 0x4d454f57; }

 private:
  // Options
  DatabaseOptions db_options_;
  Hash *hash_;
  bool is_read_only_;
  bool is_closed_;
  FileType filetype_default_;
  std::mutex mutex_close_;

  uint32_t fileid_;
  uint32_t sequence_fileid_;
  std::mutex mutex_sequence_fileid_;

  uint64_t timestamp_;
  uint64_t sequence_timestamp_;
  std::mutex mutex_sequence_timestamp_;
  bool is_locked_sequence_timestamp_;

  int size_block_;
  bool has_file_;
  bool has_sync_option_;
  int fd_;
  std::string filepath_;
  uint64_t offset_start_;
  uint64_t offset_end_;
  std::string dbname_;
  char *buffer_raw_;
  char *buffer_index_;
  bool buffer_has_items_;
  kdb::CRC32 crc32_;
  std::string prefix_;
  std::string prefix_compaction_;
  std::string dirpath_locks_;
  bool wait_until_can_open_new_files_;

 public:
  FileResourceManager file_resource_manager;

  // key_to_location is made to be dependent on the id of the thread that
  // originated an order, so that if two writers simultaneously write entries
  // with the same key, they will be properly stored into separate locations.
  // NOTE: is it possible for a chunk to arrive when the file is not yet
  // created, and have it's WriteMiddleOrLastChunk() fail because of that?
  // If so, need to write in buffer_raw_ instead
  // TODO-37: if a thread crashes or terminates, its data will *not* be cleaned up.
  std::map< std::thread::id, std::map<std::string, uint64_t> > key_to_location;
  std::map< std::thread::id, std::map<std::string, uint32_t> > key_to_headersize;
};

} // namespace kdb

#endif // KINGDB_HSTABLE_MANAGER_H_
