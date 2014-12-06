// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "network/server.h"

namespace kdb {

void NetworkTask::Run(std::thread::id tid, uint64_t id) {

  int bytes_received_last;
  std::regex regex_get {"get ([^\\s]*)"};
  std::regex regex_put {"set ([^\\s]*) \\d* \\d* (\\d*)\r\n"};
  std::regex regex_remove {"delete ([^\\s]*)"};

  uint32_t bytes_received_buffer = 0;
  uint32_t bytes_received_total  = 0;
  uint32_t bytes_expected = 0;
  uint64_t size_value = 0;
  uint64_t offset_value = 0;
  bool is_new = true;
  bool is_new_buffer = true;
  bool is_command_get = false;
  bool is_command_put = false;
  bool is_command_remove = false;
  char *buffer_send = new char[server_options_.size_buffer_send];
  SharedAllocatedByteArray *buffer = nullptr;
  SharedAllocatedByteArray *key = nullptr;
  int size_key = 0;
  log::trace("NetworkTask", "ENTER");
  // TODO-7: replace the memory allocation performed for 'key' and 'buffer' by a
  //         pool of pre-allocated buffers
  ReadOptions read_options;
  WriteOptions write_options;

  while (!IsStopRequested()) {
        
    // Receive the data
    log::trace("NetworkTask", "looping...");
    if (is_new) {
      log::trace("NetworkTask", "is_new");
      bytes_received_total = 0;
      bytes_expected = 0;
      size_value = 0;
      offset_value = 0;
      is_command_get = false;
      is_command_put = false;
      is_command_remove = false;
      size_key = 0;
    }

    if (is_new_buffer) {
      log::trace("NetworkTask", "is_new_buffer");
      bytes_received_buffer = 0;
      buffer = new SharedAllocatedByteArray(server_options_.size_buffer_recv);
      log::trace("NetworkTask", "allocated");
    }

    log::trace("NetworkTask", "Calling recv()");
    bytes_received_last = recv(sockfd_,
                               buffer->data() + bytes_received_buffer,
                               server_options_.size_buffer_recv - bytes_received_buffer,
                               0);
    if (bytes_received_last <= 0) {
      log::trace("NetworkTask", "recv()'d 0 bytes: breaking");
      break;
    }

    bytes_received_buffer += bytes_received_last;
    bytes_received_total  += bytes_received_last;
    buffer->SetOffset(0, bytes_received_buffer);

    log::trace("NetworkTask", "recv()'d %d bytes of data in buf - bytes_expected:%d bytes_received_buffer:%d bytes_received_total:%d", bytes_received_last, bytes_expected, bytes_received_buffer, bytes_received_total);

    // TODO: simplify the nested if-else blocks below to remove
    //       indentation levels

    if (is_new) {
      
      // Determine command type
      if (buffer->StartsWith("get", 3)) {
        is_command_get = true;
      } else if (buffer->StartsWith("set", 3)) {
        is_command_put = true;
      } else if (buffer->StartsWith("delete", 6)) {
        is_command_remove = true;
        log::trace("NetworkTask", "got delete command");
      } else if (buffer->StartsWith("quit", 4)) {
        break;
      }

      // Determine bytes_expected
      if (is_command_put) {
        uint64_t offset_end_key = 4; // skipping 'set '
        while (buffer->data()[offset_end_key] != ' ') offset_end_key++;

        delete key; // TODO: Should be placed at the beginning of the "if (is_new)"
                    //       so that the keys could be cleaned up for any new
                    //       command and not just for put.
        key = new SharedAllocatedByteArray();
        *key = *buffer;
        key->SetOffset(4, offset_end_key-4);

        offset_value = offset_end_key;
        while (buffer->data()[offset_value] != '\n') offset_value++;
        offset_value++; // for the \n

        log::trace("NetworkTask", "offset_value %" PRIu64, offset_value);

        std::smatch matches;
        std::string str_buffer(buffer->data(), offset_value);
        if (std::regex_search(str_buffer, matches, regex_put)) {
          size_value = atoi(std::string(matches[2]).c_str());
          bytes_expected = offset_value + size_value + 2;
          std::string str_debug = std::string(matches[2]);
          log::trace("NetworkTask", "[%s] expected [%s] [%" PRIu64 "]", key->ToString().c_str(), str_debug.c_str(), bytes_expected);
          // +2: because of the final \r\n
        } else {
          // should never happen, keeping it here until fully tested
          log::emerg("NetworkTask", "Could not match put command [%s]", str_buffer.c_str());
          break;
          //exit(-1);
        }
      } else if (   bytes_received_last >= 2
                 && buffer->data()[bytes_received_last-2] == '\r'
                 && buffer->data()[bytes_received_last-1] == '\n') {
        bytes_expected = bytes_received_last;
      } else {
        // should never happen, keeping it here until fully tested
        log::emerg("NetworkTask", "Don't know what to do with this new packet [%s]", buffer->ToString().c_str());
        break;
        //exit(-1);
      }
    }

    is_new = false;

    // Loop and get more data from the network if the buffer is not full and all the data
    // hasn't arrived yet
    if (   bytes_received_total < bytes_expected
        && bytes_received_buffer < server_options_.size_buffer_recv) {
      // TODO: what if the \r\n is on the two last messages, i.e. \n is the
      // first character of the last message?
      log::trace("NetworkTask", "force looping to get the rest of the data");
      is_new_buffer = false;
      continue;
    }

    log::trace("NetworkTask", "not looping, storing current buffer");

    if (is_command_get) {
      std::smatch matches;
      std::string str_buffer = buffer->ToString();
      if (std::regex_search(str_buffer, matches, regex_get)) {
        ByteArray *value = nullptr; // TODO: beware, possible memory leak here -- value is not deleted in case of break
                                    // TODO: replace the pointer with a reference
                                    //       count
        buffer->SetOffset(4, buffer->size() - 4 - 2);
        Status s = db_->Get(read_options, buffer, &value);

        if (s.IsOK()) {
          log::trace("NetworkTask", "GET: found");
          int ret = snprintf(buffer_send, server_options_.size_buffer_send, "VALUE %s 0 %" PRIu64 "\r\n", buffer->ToString().c_str(), value->size());
          if (ret < 0 || ret >= server_options_.size_buffer_send) {
            log::emerg("NetworkTask", "Network send buffer is too small"); 
          }
          log::trace("NetworkTask", "GET: buffer_send [%s]", buffer_send);
          if (send(sockfd_, buffer_send, strlen(buffer_send), 0) == -1) {
            log::trace("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }

          if (!value->is_compressed()) {
            // is this condition really necessary? can't the 'compressed' code
            // block also handle this as well?
            char *chunk;
            uint64_t size_chunk;
            s = value->data_chunk(&chunk, &size_chunk);
            if (!s.IsOK() && !s.IsDone()) {
              // TODO: this won't work, as it has to be sent before
              //       the 'VALUE' command
              if (send(sockfd_, "SERVER_ERROR Bad CRC32\r\n", 24, 0) == -1) {
                log::trace("NetworkTask", "Error: send() - %s", strerror(errno));
              }
            } else {
              if (send(sockfd_, chunk, size_chunk, 0) == -1) {
                log::trace("NetworkTask", "Error: send() - %s", strerror(errno));
              }
            }
          } else {
            // If the value is compressed
            char *chunk;
            uint64_t size_chunk;
            while (true) {
              s = value->data_chunk(&chunk, &size_chunk);
              if (s.IsDone()) break;
              if (!s.IsOK()) {
                delete[] chunk;
                log::trace("NetworkTask", "Error - data_chunk(): %s", s.ToString().c_str());
                break;
              }
              if (send(sockfd_, chunk, size_chunk, 0) == -1) {
                delete[] chunk;
                log::trace("NetworkTask", "Error: send() - %s", strerror(errno));
                break;
              }
              delete[] chunk;
            }

            if (!s.IsOK() && !s.IsDone()) {
              log::emerg("NetworkTask", "Error: send()", strerror(errno));
              //break;
            }
          }

          //if (s.IsOK() || s.IsDone()) {
            if (send(sockfd_, "\r\nEND\r\n", 7, 0) == -1) {
              log::emerg("NetworkTask", "Error: send()", strerror(errno));
              break;
            }
          //}

          /*
          if (send(sockfd_, value->data(), value->size(), 0) == -1) {
            log::trace("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }
          if (send(sockfd_, "\r\nEND\r\n", 7, 0) == -1) {
            log::emerg("NetworkTask", "Error: send()", strerror(errno));
            break;
          }
          */
        } else {
          log::trace("NetworkTask", "GET: [%s]", s.ToString().c_str());
          std::string msg = "NOT_FOUND\r\n";
          if (send(sockfd_, msg.c_str(), msg.length(), 0) == -1) {
            log::emerg("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }
        }
        is_new = true;
        is_new_buffer = true;
        delete value;
        delete buffer;
      } else {
        log::emerg("NetworkTask", "Could not match Get command");
        break;
      }
    } else if (is_command_remove) {
      std::smatch matches;
      std::string str_buffer = buffer->ToString();
      if (std::regex_search(str_buffer, matches, regex_remove)) {
        buffer->SetOffset(7, buffer->size() - 7 - 2);
        Status s = db_->Remove(write_options, buffer);
        if (s.IsOK()) {
          // TODO: check for [noreply], which may be present (see Memcached
          // protocol specs)
          log::trace("NetworkTask", "REMOVE: ok");
          if (send(sockfd_, "DELETED\r\n", 9, 0) == -1) {
            log::emerg("NetworkTask", "Error - send() %s", strerror(errno));
            break;
          }
        } else {
          log::emerg("NetworkTask", "Remove() error: [%s]", s.ToString().c_str());
          break;
        }
        is_new = true;
        is_new_buffer = true;
      } else {
        log::emerg("NetworkTask", "Could not match Remove command");
        break;
      }
    } else if (is_command_put) {
      uint64_t offset_chunk;
      SharedAllocatedByteArray *chunk = buffer;

      if(bytes_received_total == bytes_received_buffer) {
        // chunk is a first chunk, need to skip all the characters before the
        // value data
        chunk->SetOffset(offset_value, bytes_received_buffer - offset_value);
        offset_chunk = 0;
      } else {
        chunk->SetOffset(0, bytes_received_buffer);
        offset_chunk = bytes_received_total - bytes_received_buffer - offset_value;
      }

      if (bytes_received_total == bytes_expected) {
        // chunk is a last chunk
        // in case this is the last buffer, the size of the buffer needs to be
        // adjusted to ignore the final \r\n
        chunk->AddSize(-2);
      }

      if (chunk->size() > 0) {
        // TODO-8: make sure that 'key_current' is not created as a new allocated
        // ByteArray but just a shared copy of 'key' -- there is currently a bug
        // in the ByteArray code so new allocated is an acceptable temporary
        // solution. Once the bug is fixed, memory must be shared.
        ByteArray *key_current = new SharedAllocatedByteArray(key->size());
        memcpy(key_current->data(), key->data(), key->size());
        log::trace("NetworkTask", "call PutChunk key [%s] bytes_received_buffer:%" PRIu64 " bytes_received_total:%" PRIu64 " bytes_expected:%" PRIu64 " size_chunk:%" PRIu64, key->ToString().c_str(), bytes_received_buffer, bytes_received_total, bytes_expected, chunk->size());
        Status s = db_->PutChunk(write_options,
                                 key_current,
                                 chunk,
                                 offset_chunk,
                                 size_value);
        if (!s.IsOK()) {
          log::trace("NetworkTask", "Error - Put(): %s", s.ToString().c_str());
        } else {
          buffer = nullptr;
        }
      }

      if (bytes_received_total == bytes_expected) {
        is_new = true;
        log::trace("NetworkTask", "STORED key [%s] bytes_received_buffer:%" PRIu64 " bytes_received_total:%" PRIu64 " bytes_expected:%" PRIu64, key->ToString().c_str(), bytes_received_buffer, bytes_received_total, bytes_expected);
        if (send(sockfd_, "STORED\r\n", 8, 0) == -1) {
          log::emerg("NetworkTask", "Error - send() %s", strerror(errno));
          break;
        }
      }
      is_new_buffer = true;
    } else {
      // for debugging
      log::emerg("NetworkTask", "Unknown case for buffer");
      //exit(-1);
    }
  }
  log::trace("NetworkTask", "exit and close socket");

  delete key;
  delete buffer;
  delete[] buffer_send;
  close(sockfd_);
}


void* Server::GetSockaddrIn(struct sockaddr *sa)
{
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in*)sa)->sin_addr);
  }
  return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


Status Server::Start(ServerOptions& server_options,
                     DatabaseOptions& db_options,
                     std::string& dbname) {
  server_options_ = server_options;
  db_options_ = db_options;
  dbname_ = dbname;
  thread_network_ = std::thread(&Server::AcceptNetworkTraffic, this);
  return Status::OK();
}

void Server::AcceptNetworkTraffic() {

  // Create the database object and the thread pool
  db_ = new kdb::KingDB(db_options_, dbname_);
  Status s = db_->Open();
  if (!s.IsOK()) {
    log::emerg("Server", s.ToString().c_str()); 
    stop_requested_ = true;
    return;
  }
  tp_ = new ThreadPool(server_options_.num_threads);
  tp_->Start();
  log::trace("Server", "waiting for connections...");

  // Ignoring SIGPIPE, which would crash the program when writing to
  // a broken socket -- doing this because MSG_NOSIGNAL doesn't work on Mac OS X
  signal(SIGPIPE, SIG_IGN);

  struct addrinfo ai_hints, *ai_server, *ai_ptr;
  memset(&ai_hints, 0, sizeof(ai_hints));
  ai_hints.ai_family = AF_UNSPEC;
  ai_hints.ai_socktype = SOCK_STREAM;
  ai_hints.ai_flags = AI_PASSIVE;
  std::string str_port = std::to_string(server_options_.interface__memcached_port);
  int ret;
  if ((ret = getaddrinfo(NULL, str_port.c_str(), &ai_hints, &ai_server)) != 0) {
    log::emerg("Server", "getaddrinfo: %s", gai_strerror(ret)); 
    stop_requested_ = true;
    return;// Status::IOError("Server - getaddrinfo", gai_strerror(ret));
  }

  // Bind to the first result
  int sockfd_listen;
  for(ai_ptr = ai_server; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next) {
    if ((sockfd_listen = socket(ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol)) == -1) {
      continue;
    }

    int setsockopt_yes=1;
    if (setsockopt(sockfd_listen, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1) {
      log::emerg("Server", "setsockopt: %s", strerror(errno)); 
      stop_requested_ = true;
      freeaddrinfo(ai_server);
      return;// Status::IOError("Server - setsockopt", strerror(errno));
    }

    if (bind(sockfd_listen, ai_ptr->ai_addr, ai_ptr->ai_addrlen) == -1) {
      continue;
    }

    break;
  }

  freeaddrinfo(ai_server);

  if (ai_ptr == NULL) {
    log::emerg("Server", "Failed to bind()");
    stop_requested_ = true;
    return;// Status::IOError("Server - Failed to bind");
  }

  if (listen(sockfd_listen, server_options_.listen_backlog) == -1) {
    log::emerg("Server", "listen(): %s", strerror(errno));
    stop_requested_ = true;
    return;// Status::IOError("Server - listen", strerror(errno));
  }

  sockfd_listen_ = sockfd_listen;

  // Create notification pipe
  int pipefd[2];
  if(pipe(pipefd) < 0) {
    stop_requested_ = true;
    return;
  }
  sockfd_notify_recv_ = pipefd[0];
  sockfd_notify_send_ = pipefd[1];
  fcntl(sockfd_notify_send_, F_SETFL, O_NONBLOCK);

  fd_set sockfds_read;
  int sockfd_max = std::max(sockfd_notify_recv_, sockfd_listen) + 1;

  // Start accepting connections
  int sockfd_accept;
  struct sockaddr_storage sockaddr_client;
  socklen_t size_sa;
  char address[INET6_ADDRSTRLEN];
  while (!IsStopRequested()) {
    FD_ZERO(&sockfds_read);
    FD_SET(sockfd_notify_recv_, &sockfds_read);
    FD_SET(sockfd_listen, &sockfds_read);

    log::trace("Server", "select()");
    size_sa = sizeof(sockaddr_client);
    int ret_select = select(sockfd_max, &sockfds_read, NULL, NULL, NULL);
    if (ret_select < 0) {
      log::emerg("Server", "select() error %s", strerror(errno));
      stop_requested_ = true;
      return;
    } else if (ret_select == 0) {
      continue;
    }

    if (!FD_ISSET(sockfd_listen, &sockfds_read)) continue;

    log::trace("Server", "accept()");
    sockfd_accept = accept(sockfd_listen, (struct sockaddr *)&sockaddr_client, &size_sa);
    if (sockfd_accept == -1) continue;

    inet_ntop(sockaddr_client.ss_family,
              GetSockaddrIn((struct sockaddr *)&sockaddr_client),
              address,
              sizeof(address));
    log::trace("Server", "got connection from %s\n", address);

    tp_->AddTask(new NetworkTask(sockfd_accept, server_options_, db_));
  }
  log::trace("Server", "Exiting thread");
}

} // end of namespace kdb
