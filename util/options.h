// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_OPTIONS_H_
#define KINGDB_OPTIONS_H_

#include "util/debug.h"
#include "config_parser.h"

namespace kdb {

enum HashType {
  kMurmurHash3_64 = 0x0,
  kxxHash_64      = 0x1
};

enum CompressionType {
  kNoCompression  = 0x0,
  kLZ4Compression = 0x1
};

enum ChecksumType {
  kNoChecksum    = 0x0,
  kCrc32Checksum = 0x1
};

struct CompressionOptions {
  CompressionOptions(CompressionType ct)
      : type(ct) {
  }
  CompressionType type;
};

struct DatabaseOptions {
 public:
  DatabaseOptions()
      : internal__hstable_header_size(8192),
        hash(kxxHash_64),
        compression(kLZ4Compression),
        checksum(kCrc32Checksum) {
    DatabaseOptions &db_options = *this;
    ConfigParser parser;
    AddParametersToConfigParser(db_options, parser);
    parser.LoadDefaultValues();
  }

  // Internal options (part of the file format, cannot be changed by users)
  uint64_t internal__hstable_header_size;

  // Constant options (cannot be changed after the db is created)
  HashType hash;
  CompressionOptions compression;
  ChecksumType checksum;
  uint64_t storage__hstable_size;
  std::string storage__compression_algorithm;
  std::string storage__hashing_algorithm;

  // Instance options (can be changed each time the db is opened)
  bool create_if_missing;
  bool error_if_exists;
  uint32_t max_open_files; // TODO: this parameter is ignored: use it

  uint64_t write_buffer__size;
  uint64_t write_buffer__flush_timeout;
  uint64_t write_buffer__close_timeout;

  uint64_t storage__streaming_timeout;
  uint64_t storage__statistics_polling_interval;
  uint64_t storage__free_space_reject_orders;
  uint64_t storage__maximum_chunk_size;
  uint64_t storage__num_index_iterations_per_lock;

  uint64_t compaction__check_interval;
  uint64_t compaction__filesystem__survival_mode_threshold;
  uint64_t compaction__filesystem__normal_batch_size;
  uint64_t compaction__filesystem__survival_batch_size;
  uint64_t compaction__filesystem__free_space_required;

  static std::string GetPath(const std::string &dirpath) {
    return dirpath + "/db_options";
  }

  static std::string GetFilename() {
    return "db_options";
  }

  static void AddParametersToConfigParser(DatabaseOptions& db_options, ConfigParser& parser) {

    // Database options
    parser.AddParameter(new kdb::BooleanParameter(
                         "db.create-if-missing", true, &db_options.create_if_missing, false,
                         "Will create the database if it does not already exists"));
    parser.AddParameter(new kdb::BooleanParameter(
                         "db.error-if-exists", false, &db_options.error_if_exists, false,
                         "Will exit if the database already exists"));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.write-buffer.size", "32MB", &db_options.write_buffer__size, false,
                         "Size of the Write Buffer. The database has two of these buffers."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.write-buffer.flush-timeout", "500 milliseconds", &db_options.write_buffer__flush_timeout, false,
                         "The timeout after which the write buffer will flush its cache."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.write-buffer.close-timeout", "5 seconds", &db_options.write_buffer__close_timeout, false,
                         "The time that a closing process will ahave to wait when flushing the vectors in the Writer Buffer."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.hstable-size", "32MB", &db_options.storage__hstable_size, false,
                         "Maximum size a HSTable can have. Entries with keys and values beyond that size are considered to be large entries."));
    parser.AddParameter(new kdb::StringParameter(
                         "db.storage.compression", "lz4", &db_options.storage__compression_algorithm, false,
                         "Compression algorithm used by the storage engine. Can be 'disabled' or 'lz4'."));
    parser.AddParameter(new kdb::StringParameter(
                         "db.storage.hashing", "xxhash-64", &db_options.storage__hashing_algorithm, false,
                         "Hashing algorithm used by the storage engine. Can be 'xxhash-64' or 'murmurhash3-64'."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.free-space-reject-orders", "192MB", &db_options.storage__free_space_reject_orders, false,
                         "Free space below which new incoming orders are rejected. Should be at least (2 * 'db.write-buffer.size' + 4 * 'db.hstable.maximum-size'), so that when the file system fills up, the two write buffers can be flushed to secondary storage safely and the survival-mode compaction process can be run."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.maximum-chunk-size", "1MB", &db_options.storage__maximum_chunk_size, false,
                         "The maximum chunk size is used by the storage engine to cut entries into smaller chunks -- important for the compression and hashing algorithms, can never be more than (2^32 - 1) as the algorihms used do not support sizes above that value."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.timeout-streaming", "60 seconds", &db_options.storage__streaming_timeout, false,
                         "The time of inactivity after which an entry stored with the streaming API is considered left for dead, and any subsequent incoming chunks for that entry are rejected."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.statistics-polling-interval", "5 seconds", &db_options.storage__statistics_polling_interval, false,
                         "The frequency at which statistics are polled in the Storage Engine (free disk space, etc.)."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.num-index-iterations-per-lock", "10", &db_options.storage__num_index_iterations_per_lock, false,
                         "Number of entries merged into the Storage Engine index for each locking of the dedicated mutex. This parameter throttles index updates."));

    // Compaction options
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.check-interval", "30 seconds", &db_options.compaction__check_interval, false,
                         "The frequency at which the compaction conditions are checked."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.filesystem.free-space-required", "128MB", &db_options.compaction__filesystem__free_space_required, false,
                         "Minimum free space on the file system required for a compaction process to be started."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.filesystem.survival-mode-threshold", "2GB", &db_options.compaction__filesystem__survival_mode_threshold, false,
                         "If the free space on the file system is above that threshold, the compaction is in 'normal mode'. Below that threshold, the compaction is in 'survival mode'. Each mode triggers the compaction process for different amount of uncompacted data found in the database."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.filesystem.normal-batch-size", "1GB", &db_options.compaction__filesystem__normal_batch_size, false,
                         "If the compaction is in normal mode and the amount of uncompacted data is above that value of 'normal-batch-size', then the compaction will start when the compaction conditions are checked."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.filesystem.survival-batch-size", "256MB", &db_options.compaction__filesystem__survival_batch_size, false,
                         "If the compaction is in survival mode and the amount of uncompacted data is above that value of 'survival-batch-size', then the compaction will start when the compaction conditions are checked."));


  }
    
};

struct ReadOptions {
  bool verify_checksums;
  bool fill_cache;
  ReadOptions()
      : verify_checksums(false),
        fill_cache(true) {
  }
};

struct WriteOptions {
  bool sync;
  WriteOptions()
      : sync(false) {
  }
};

struct ServerOptions {
  ServerOptions() {}
  uint32_t interface__memcached_port;
  uint32_t listen_backlog;
  uint32_t num_threads;
  uint64_t size_buffer_recv;
  uint64_t size_buffer_send;

  static void AddParametersToConfigParser(ServerOptions& server_options, ConfigParser& parser) {
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "server.size-buffer-recv", "64KB", &server_options.size_buffer_recv, false,
                         "Size of the buffer used to receive data from the network. Each thread of the server has one such buffer."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "server.size-buffer-send", "1KB", &server_options.size_buffer_send, false,
                         "Size of send buffer."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.listen-backlog", "150", &server_options.listen_backlog, false,
                         "Size of the listen() backlog."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.num-threads", "150", &server_options.num_threads, false,
                         "Num of threads in the pool of workers."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.interface.memcached-port", "3490", &server_options.interface__memcached_port, false,
                         "Port where the memcached interface will listen."));
  }

};

} // namespace kdb

#endif // KINGDB_OPTIONS_H_
