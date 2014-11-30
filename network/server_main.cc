// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.
//
#include <execinfo.h>
#include <csignal>

#include "network/server.h"
#include "thread/threadpool.h"
#include "util/options.h"
#include "util/file.h"
#include "util/config_parser.h"

void show_usage(char *program_name) {
  printf("Example: %s --db-name mydb --port 3490 --backlog 150 --num-threads 150\n", program_name);
}

bool stop_requested = false;

void crash_signal_handler(int sig) {
  int depth_max = 20;
  void *array[depth_max];
  size_t depth;

  depth = backtrace(array, depth_max);
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, depth, STDERR_FILENO);
  exit(1);
}

void termination_signal_handler(int signal) {
  fprintf(stderr, "Received signal [%d]\n", signal);
  stop_requested = true; 
}

int main(int argc, char** argv) {

  kdb::Status s;
  std::string dbname = "";
  std::string log_level = "";
  std::string configfile = "";
  kdb::ServerOptions server_options;
  kdb::DatabaseOptions db_options;

  // Looking for '--configfile'
  kdb::ConfigParser parser_configfile;
  parser_configfile.error_if_unknown_parameters = false;
  parser_configfile.AddParameter(new kdb::StringParameter(
                                 "configfile", "", &configfile, false,
                                 "Configuration file. If not specified, the path ./kingdb.conf and /etc/kingdb.conf will be tested."));

  s = parser_configfile.ParseCommandLine(argc, argv);
  if (!s.IsOK()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    exit(-1);
  }
  
  struct stat info;
  if (configfile == "") {
    if (stat("./kingdb.conf", &info) == 0) {
      configfile = "./kingdb.conf";
    } else if (stat("/etc/kingdb.conf", &info) == 0) {
      configfile = "/etc/kingdb.conf";
    }
  } else if (stat(configfile.c_str(), &info) != 0) {
    fprintf(stderr, "Could not file configuration file [%s]\n", configfile.c_str());
    exit(-1);
  }

  // Now parsing all options
  kdb::ConfigParser parser;

  // General options
  parser.AddParameter(new kdb::StringParameter(
                      "configfile", "", &configfile, false,
                      "Configuration file. If not specified, the path ./kingdb.conf and /etc/kingdb.conf will be tested."));
  parser.AddParameter(new kdb::StringParameter(
                      "log_level", "trace", &log_level, false,
                      "Level of the logging, can be: emerg, alert, crit, error, warn, notice, info, debug, trace."));
  parser.AddParameter(new kdb::StringParameter(
                      "db.path", "", &dbname, true,
                      "Path where the database can be found or will be created."));

  kdb::ServerOptions::AddParametersToConfigParser(server_options, parser);
  kdb::DatabaseOptions::AddParametersToConfigParser(db_options, parser);

  if (configfile != "") {
    s = parser.ParseFile(configfile); 
    if (!s.IsOK()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      exit(-1);
    }
  }

  s = parser.ParseCommandLine(argc, argv);
  if (!s.IsOK()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    exit(-1);
  }

  if (!parser.FoundAllMandatoryParameters()) {
    parser.PrintAllMissingMandatoryParameters();
    exit(-1);
  }

  if (log_level != "" && kdb::Logger::set_current_level(log_level.c_str()) < 0) {
    fprintf(stderr, "Unknown log level: [%s]\n", log_level.c_str());
    exit(-1);
  }

  kdb::CompressionType ctype;
  if (db_options.storage__compression_algorithm == "disabled") {
    ctype = kdb::kNoCompression;
  } else if (db_options.storage__compression_algorithm == "lz4") {
    ctype = kdb::kLZ4Compression;
  } else {
    fprintf(stderr, "Unknown compression algorithm: [%s]\n", db_options.storage__compression_algorithm.c_str());
    exit(-1);
  }
  db_options.compression = ctype;

  kdb::HashType htype;
  if (db_options.storage__hashing_algorithm == "xxhash_64") {
    htype = kdb::kxxHash_64;
  } else if (db_options.storage__hashing_algorithm == "murmurhash3_64") {
    htype = kdb::kMurmurHash3_64;
  } else {
    fprintf(stderr, "Unknown hashing algorithm: [%s]\n", db_options.storage__hashing_algorithm.c_str());
    exit(-1);
  }
  db_options.hash = htype;

  kdb::FileUtil::increase_limit_open_files();

  if (dbname == "") {
    fprintf(stderr, "Missing arguments\n");
    exit(-1); 
  }

#ifndef DEBUG
  signal(SIGINT, termination_signal_handler);
  signal(SIGTERM, termination_signal_handler);

  signal(SIGSEGV, crash_signal_handler);
  signal(SIGABRT, crash_signal_handler);
#endif

  kdb::Server server;
  server.Start(server_options, db_options, dbname);
  while (!stop_requested && !server.IsStopRequested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  server.Stop();
  return 0;
}
