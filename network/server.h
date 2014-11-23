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
  kdb::ServerOptions server_options_;
  kdb::KingDB *db_;
  NetworkTask(int sockfd, kdb::ServerOptions server_options, kdb::KingDB* db) {
    sockfd_ = sockfd;
    server_options_ = server_options;
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
        sockfd_listen_(0),
        sockfd_notify_recv_(0),
        sockfd_notify_send_(0),
        db_(nullptr),
        tp_(nullptr)
  {}

  Status Start(ServerOptions& server_options,
               DatabaseOptions& db_options,
               std::string& dbname);
  void AcceptNetworkTraffic();
  bool IsStopRequested() { return stop_requested_; }
  void Stop() {
    log::trace("Server", "Stop()");
    stop_requested_ = true;
    if (sockfd_notify_send_ > 0) {
      if (write(sockfd_notify_send_, "0", 1) < 0) {
        log::trace("Server",
                  "Could not send the stop notification to the server thread: %s.",
                  strerror(errno));
      }
    }
    thread_network_.join();
    if (tp_ != nullptr) {
      tp_->Stop();
      delete tp_;
    }
    if (db_ != nullptr) {
      db_->Close();
      delete db_;
    }
    if (sockfd_listen_ > 0) close(sockfd_listen_);
    if (sockfd_notify_recv_ > 0) close(sockfd_notify_recv_);
    if (sockfd_notify_send_ > 0) close(sockfd_notify_send_);
  }


 private:
  void* GetSockaddrIn(struct sockaddr *sa);
  bool stop_requested_;
  std::thread thread_network_;

  ServerOptions server_options_;
  DatabaseOptions db_options_;
  std::string dbname_;

  int sockfd_listen_;
  int sockfd_notify_recv_;
  int sockfd_notify_send_;

  kdb::KingDB* db_;
  ThreadPool *tp_;
};

} // end of namespace kdb

#endif // KINGDB_SERVER_H_
