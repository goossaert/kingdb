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
  kNoChecksum = 0x0,
  kCRC32C     = 0x1  // CRC-32C (Castagnoli)
};

enum WriteBufferMode {
  kWriteBufferModeDirect   = 0x0, 
  kWriteBufferModeAdaptive = 0x1
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
      : internal__hstable_header_size(8192),          // bytes
        internal__num_iterations_per_lock(10),
        internal__close_timeout(500),                 // milliseconds
        internal__open_file_retry_delay(5000),        // milliseconds
        internal__size_multipart_required(1024*1024), // bytes
        internal__compaction_check_interval(500),     // milliseconds
        hash(kxxHash_64),
        compression(kLZ4Compression),
        checksum(kCRC32C),
        write_buffer__mode(kWriteBufferModeDirect) {
    DatabaseOptions &db_options = *this;
    ConfigParser parser;
    AddParametersToConfigParser(db_options, parser);
    parser.LoadDefaultValues();
  }

  // *** Internal options (part of the file format, cannot be changed by users)
  uint64_t internal__hstable_header_size;

  // Wherever a loop needs to lock to merge or process data, this is the 
  // number of iterations done for each locking of the dedicated mutex.
  // This allows to throttle the locking and prevents the starvation
  // of other processes.
  uint64_t internal__num_iterations_per_lock; 

  // The time that a closing process will have to wait when flushing the vectors
  // in the Writer Buffer.
  uint64_t internal__close_timeout;

  // If a file fails to open in the HSTableManager, this is the delay that
  // will be waited before retrying to open the file.
  uint64_t internal__open_file_retry_delay;

  // Size of an entry value for which using a multipart is required. Below this
  // size, a byte array will be allocated an the entry will be uncompressed
  // in it.
  uint64_t internal__size_multipart_required;

  // The frequency at which the compaction conditions are checked.
  uint64_t internal__compaction_check_interval;


  // *** Constant options (cannot be changed after the db is created)
  HashType hash;
  CompressionOptions compression;
  ChecksumType checksum;
  uint64_t storage__hstable_size;
  std::string storage__compression_algorithm;
  std::string storage__hashing_algorithm;


  // *** Instance options (can be changed each time the db is opened)
  bool create_if_missing;
  bool error_if_exists;
  uint32_t max_open_files; // TODO: this parameter is ignored: use it

  uint64_t rate_limit_incoming;

  uint64_t write_buffer__size;
  uint64_t write_buffer__flush_timeout;
  std::string write_buffer__mode_str;
  WriteBufferMode write_buffer__mode;

  uint64_t storage__inactivity_timeout;
  uint64_t storage__statistics_polling_interval;
  uint64_t storage__minimum_free_space_accept_orders;
  uint64_t storage__maximum_part_size;

  uint64_t compaction__force_interval;
  uint64_t compaction__filesystem__survival_mode_threshold;
  uint64_t compaction__filesystem__normal_batch_size;
  uint64_t compaction__filesystem__survival_batch_size;
  uint64_t compaction__filesystem__free_space_required;

  // *** Logging
  std::string log_level;
  std::string log_target;

  static std::string GetPath(const std::string &dirpath) {
    return dirpath + "/db_options";
  }

  static std::string GetFilename() {
    return "db_options";
  }

  static void AddParametersToConfigParser(DatabaseOptions& db_options, ConfigParser& parser) {

    // Logging options
    parser.AddParameter(new kdb::StringParameter(
                        "log.level", "info", &db_options.log_level, false,
                        "Level of the logging, can be: silent, emerg, alert, crit, error, warn, notice, info, debug, trace."));
    parser.AddParameter(new kdb::StringParameter(
                        "log.target", "kingdb", &db_options.log_target, false,
                        "Target of the logs, can be 'stderr' to log to stderr, or any custom string that will be used as the 'ident' parameter for syslog."));

    // Database options
    parser.AddParameter(new kdb::BooleanParameter(
                         "db.create-if-missing", true, &db_options.create_if_missing, false,
                         "Will create the database if it does not already exists"));
    parser.AddParameter(new kdb::BooleanParameter(
                         "db.error-if-exists", false, &db_options.error_if_exists, false,
                         "Will exit if the database already exists"));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.incoming-rate-limit", "0", &db_options.rate_limit_incoming, false,
                         "Limit the rate of incoming traffic, in bytes per second. Unlimited if equal to 0."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.write-buffer.size", "64MB", &db_options.write_buffer__size, false,
                         "Size of the Write Buffer."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.write-buffer.flush-timeout", "500 milliseconds", &db_options.write_buffer__flush_timeout, false,
                         "The timeout after which the write buffer will flush its cache."));
    parser.AddParameter(new kdb::StringParameter(
                         "db.write-buffer.mode", "direct", &db_options.write_buffer__mode_str, false,
                         "The mode with which the write buffer handles incoming traffic, can be 'direct' or 'adaptive'. With the 'direct' mode, once the Write Buffer is full other incoming Write and Delete operations will block until the buffer is persisted to secondary storage. The direct mode should be used when the clients are not subjects to timeouts. When choosing the 'adaptive' mode, incoming orders will be made slower, down to the speed of the writes on the secondary storage, so that they are almost just as fast as when using the direct mode, but are never blocking. The adaptive mode is expected to introduce a small performance decrease, but required for cases where clients timeouts must be avoided, for example when the database is used over a network."));
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
                         "db.storage.minimum-free-space-accept-orders", "192MB", &db_options.storage__minimum_free_space_accept_orders, false,
                         "Minimum free disk space required to accept incoming orders. It is recommended that for this value to be at least (2 x 'db.write-buffer.size' + 4 x 'db.hstable.maximum-size'), so that when the file system fills up, the two write buffers can be flushed to secondary storage safely and the survival-mode compaction process can be run."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.maximum-part-size", "1MB", &db_options.storage__maximum_part_size, false,
                         "The maximum part size is used by the storage engine to split entries into smaller parts -- important for the compression and hashing algorithms, can never be more than (2^32 - 1) as the algorihms used do not support sizes above that value."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.inactivity-streaming", "60 seconds", &db_options.storage__inactivity_timeout, false,
                         "The time of inactivity after which an entry stored with the streaming API is considered left for dead, and any subsequent incoming parts for that entry are rejected."));
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.storage.statistics-polling-interval", "5 seconds", &db_options.storage__statistics_polling_interval, false,
                         "The frequency at which statistics are polled in the Storage Engine (free disk space, etc.)."));


    // Compaction options
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "db.compaction.force-interval", "5 minutes", &db_options.compaction__force_interval, false,
                         "Duration after which, if no compaction process has been performed, a compacted is started. Set to 0 to disable."));
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
  ReadOptions()
      : verify_checksums(false) {
  }
};

struct WriteOptions {
  bool sync;
  WriteOptions()
      : sync(false) {
  }
};

struct ServerOptions {
  ServerOptions()
    : internal__size_buffer_send(1024) {
  }

  // *** Internal options
  uint64_t internal__size_buffer_send;

  uint32_t interface__memcached_port;
  uint32_t listen_backlog;
  uint32_t num_threads;
  uint64_t size_buffer_recv;


  // TODO: change size-buffer-recv to recv-socket-buffer-size ?
  // TODO: add a max-connections parameter?

  static void AddParametersToConfigParser(ServerOptions& server_options, ConfigParser& parser) {
    parser.AddParameter(new kdb::UnsignedInt64Parameter(
                         "server.size-buffer-recv", "64KB", &server_options.size_buffer_recv, false,
                         "Size of the buffer used to receive data from the network. Each thread of the server has one such buffer."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.listen-backlog", "150", &server_options.listen_backlog, false,
                         "Size of the listen() backlog."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.num-threads", "150", &server_options.num_threads, false,
                         "Num of threads in the pool of workers."));
    parser.AddParameter(new kdb::UnsignedInt32Parameter(
                         "server.interface.memcached-port", "11211", &server_options.interface__memcached_port, false,
                         "Port where the memcached interface will listen."));
  }

};

} // namespace kdb

#endif // KINGDB_OPTIONS_H_
