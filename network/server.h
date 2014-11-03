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
#include <sys/select.h>
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
#include <chrono>

#include "util/options.h"
#include "util/logger.h"
#include "kingdb/kdb.h"
#include "thread/threadpool.h"
#include "interface/kingdb.h"
#include "util/byte_array.h"


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

  virtual void Run(std::thread::id tid, uint64_t id);

};


class Server {
 public:
  Server()
      : stop_requested_(false),
        tp_(nullptr),
        db_(nullptr)
  {}

  Status Start(DatabaseOptions& options,
               std::string& dbname,
               int port,
               int backlog,
               int num_threads);
  void AcceptNetworkTraffic();
  bool IsStopRequested() { return stop_requested_; }
  void Stop() {
    LOG_TRACE("Server", "Stop()");
    stop_requested_ = true;
    write(sockfd_notify_send_, "0", 1);
    thread_network_.join();
    tp_->Stop();
    db_->Close();
    delete tp_;
    delete db_;
    close(sockfd_listen_);
    close(sockfd_notify_recv_);
    close(sockfd_notify_send_);
  }


 private:
  void* GetSockaddrIn(struct sockaddr *sa);
  bool stop_requested_;
  std::thread thread_network_;

  DatabaseOptions options_;
  std::string dbname_;
  int port_;
  int backlog_;
  int num_threads_;

  int sockfd_listen_;
  int sockfd_notify_recv_;
  int sockfd_notify_send_;

  kdb::KingDB* db_;
  ThreadPool *tp_;
};

} // end of namespace kdb

#endif // KINGDB_SERVER_H_
