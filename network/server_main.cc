// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include <sys/resource.h>
#include "network/server.h"
#include "thread/threadpool.h"
#include "util/options.h"

void show_usage(char *program_name) {
  printf("Example: %s --db-name mydb --port 3490 --backlog 150 --num-threads 150\n", program_name);
}

void increase_limit_open_files() {
  struct rlimit rl;
  if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
    rl.rlim_cur = OPEN_MAX;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
      fprintf(stderr, "Could not increase the limit on open files for this process");
    }
  }
}

bool stop_requested = false;

void termination_signal_handler(int signal) {
  fprintf(stderr, "Received signal [%d]\n", signal);
  stop_requested = true; 
}

int main(int argc, char** argv) {
  if (argc == 1) {
    show_usage(argv[0]); 
    exit(0);
  }

  if (argc % 2 == 0) {
    show_usage(argv[0]); 
    std::cerr << "Error: invalid number of arguments" << std::endl; 
    exit(-1);
  }

  increase_limit_open_files();

  int port = 0;
  int backlog = 0;
  int num_threads = 0;
  std::string dbname = "";
  kdb::CompressionType ctype = kdb::kLZ4Compression;

  if (argc > 2) {
    for (int i = 1; i < argc; i += 2 ) {
      if (strcmp(argv[i], "--port" ) == 0) {
        port = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--backlog" ) == 0) {
        backlog = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--num-threads" ) == 0) {
        num_threads = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--db-name" ) == 0) {
        dbname = std::string(argv[i+1]);
      } else if (strcmp(argv[i], "--log-level" ) == 0) {
        if (kdb::Logger::set_current_level(argv[i+1]) < 0 ) {
          fprintf(stderr, "Unknown log level: [%s]\n", argv[i+1]);
          exit(-1); 
        }
      } else if (strcmp(argv[i], "--compression" ) == 0) {
        std::string compression(argv[i+1]);
        if (compression == "disabled") {
          ctype = kdb::kNoCompression;
        } else if (compression == "lz4") {
          ctype = kdb::kLZ4Compression;
        } else {
          fprintf(stderr, "Unknown compression option: [%s]\n", argv[i+1]);
          exit(-1); 
        }
      } else {
        fprintf(stderr, "Unknown parameter [%s]\n", argv[i]);
        exit(-1); 
      }
    }
  }

  if (port == 0 || backlog == 0 || num_threads == 0 || dbname == "") {
    fprintf(stderr, "Missing arguments\n");
    exit(-1); 
  }

  kdb::DatabaseOptions options;
  options.compression = ctype;

  signal(SIGINT, termination_signal_handler);
  signal(SIGTERM, termination_signal_handler);
  kdb::Server server;
  server.Start(options, dbname, port, backlog, num_threads);
  while (!stop_requested) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000*1000));
  }
  server.Stop();
  return 0;
}
