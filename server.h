// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_SERVER_H_
#define KINGDB_SERVER_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>

#include <iostream>
#include <thread>
#include <regex>
#include <queue>
#include <vector>
#include <string>
#include <cstdio>
#include <string.h>
#include <chrono>

#include "threadpool.h"
#include "kingdb.h"
#include "logger.h"
#include "kdb.h"
#include "byte_array.h"


namespace kdb {

class NetworkTask: public Task {
 public:
  int sockfd_;
  kdb::KingDB *db_;
  NetworkTask(int sockfd, kdb::KingDB* db) {
    sockfd_ = sockfd;
    db_ = db;
  }
  virtual ~NetworkTask() {};

  virtual void RunInLock(std::thread::id tid) {
    std::cout << "Thread " << tid << std::endl;
  }

  virtual void Run(std::thread::id tid);

};


class Server {
 public:
  Status Start(std::string dbname, int port, int backlog, int num_threads);
 private:
  void* GetSockaddrIn(struct sockaddr *sa);
};

}

#endif // KINGDB_SERVER_H_
