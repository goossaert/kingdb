// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "server.h"
#include "threadpool.h"

void show_usage(char *program_name) {
  printf("Example: %s --db-name mydb --port 3490 --backlog 150 --num-threads 150\n", program_name);
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

  int port = 0;
  int backlog = 0;
  int num_threads = 0;
  std::string dbname = "";

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

  kdb::Server server;
  server.Start(dbname, port, backlog, num_threads);
  return 0;
}
