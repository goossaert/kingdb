// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "server.h"

namespace kdb {

void NetworkTask::Run(std::thread::id tid) {

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
  char *buffer_send = new char[SIZE_BUFFER_SEND];
  SharedAllocatedByteArray *buffer = nullptr;
  SharedAllocatedByteArray *key = nullptr;
  int size_key = 0;
  LOG_TRACE("NetworkTask", "ENTER");
  // TODO: replace the memory allocation performed for 'key' and 'buffer' by a
  //       pool of pre-allocated buffers

  while(true) {
        
    // Receive the data
    LOG_TRACE("NetworkTask", "looping...");
    if (is_new) {
      LOG_TRACE("NetworkTask", "is_new");
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
      LOG_TRACE("NetworkTask", "is_new_buffer");
      bytes_received_buffer = 0;
      buffer = new SharedAllocatedByteArray(SIZE_BUFFER_RECV);
      LOG_TRACE("NetworkTask", "allocated");
    }

    LOG_TRACE("NetworkTask", "Calling recv()");
    bytes_received_last = recv(sockfd_,
                               buffer->data() + bytes_received_buffer,
                               SIZE_BUFFER_RECV - bytes_received_buffer,
                               0);
    if (bytes_received_last <= 0) {
      LOG_TRACE("NetworkTask", "recv()'d 0 bytes: breaking");
      break;
    }

    bytes_received_buffer += bytes_received_last;
    bytes_received_total  += bytes_received_last;
    buffer->SetOffset(0, bytes_received_buffer);

    LOG_TRACE("NetworkTask", "recv()'d %d bytes of data in buf - bytes_expected:%d bytes_received_buffer:%d bytes_received_total:%d", bytes_received_last, bytes_expected, bytes_received_buffer, bytes_received_total);

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
        LOG_TRACE("NetworkTask", "got delete command");
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

        LOG_TRACE("NetworkTask", "offset_value %llu", offset_value);

        std::smatch matches;
        std::string str_buffer(buffer->data(), offset_value);
        if (std::regex_search(str_buffer, matches, regex_put)) {
          size_value = atoi(std::string(matches[2]).c_str());
          bytes_expected = offset_value + size_value + 2;
          std::string str_debug = std::string(matches[2]);
          LOG_TRACE("NetworkTask", "[%s] expected [%s] [%llu]", key->ToString().c_str(), str_debug.c_str(), bytes_expected);
          // +2: because of the final \r\n
        } else {
          // should never happen, keeping it here until fully tested
          LOG_EMERG("NetworkTask", "Could not match put command [%s]", str_buffer.c_str());
          exit(-1);
        }
      } else if (   bytes_received_last >= 2
                 && buffer->data()[bytes_received_last-2] == '\r'
                 && buffer->data()[bytes_received_last-1] == '\n') {
        bytes_expected = bytes_received_last;
      } else {
        // should never happen, keeping it here until fully tested
        LOG_EMERG("NetworkTask", "Don't know what to do with this new packet [%s]", buffer->ToString().c_str());
        exit(-1);
      }
    }

    is_new = false;

    // Loop and get more data from the network if the buffer is not full and all the data
    // hasn't arrived yet
    if (   bytes_received_total < bytes_expected
        && bytes_received_buffer < SIZE_BUFFER_RECV) {
      // TODO: what if the \r\n is on the two last messages, i.e. \n is the
      // first character of the last message?
      LOG_TRACE("NetworkTask", "force looping to get the rest of the data");
      is_new_buffer = false;
      continue;
    }

    LOG_TRACE("NetworkTask", "not looping, storing current buffer");

    if (is_command_get) {
      std::smatch matches;
      std::string str_buffer = buffer->ToString();
      if (std::regex_search(str_buffer, matches, regex_get)) {
        ByteArray *value = nullptr; // TODO: beware, possible memory leak here -- value is not deleted in case of break
                                    // TODO: replace the pointer with a reference
                                    //       count
        buffer->SetOffset(4, buffer->size() - 4 - 2);
        Status s = db_->Get(buffer, &value);
        if (s.IsOK()) {
          LOG_TRACE("NetworkTask", "GET: found");
          sprintf(buffer_send, "VALUE %s 0 %llu\r\n", buffer->ToString().c_str(), value->size());
          LOG_TRACE("NetworkTask", "GET: buffer_send [%s]", buffer_send);
          if (send(sockfd_, buffer_send, strlen(buffer_send), 0) == -1) {
            LOG_TRACE("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }

          char *chunk;
          uint64_t size_chunk;
          while (true) {
            s = value->data_chunk(&chunk, &size_chunk);
            if (s.IsDone()) break;
            if (!s.IsOK()) {
              delete[] chunk;
              LOG_TRACE("NetworkTask", "Error - data_chunk(): %s", s.ToString().c_str());
              break;
            }
            if (send(sockfd_, chunk, size_chunk, 0) == -1) {
              delete[] chunk;
              LOG_TRACE("NetworkTask", "Error: send() - %s", strerror(errno));
              break;
            }
            delete[] chunk;
          }

          if (!s.IsOK() && !s.IsDone()) {
            LOG_EMERG("NetworkTask", "Error: send()", strerror(errno));
            break;
          }

          if (send(sockfd_, "\r\nEND\r\n", 7, 0) == -1) {
            LOG_EMERG("NetworkTask", "Error: send()", strerror(errno));
            break;
          }

          /*
          if (send(sockfd_, value->data(), value->size(), 0) == -1) {
            LOG_TRACE("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }
          if (send(sockfd_, "\r\nEND\r\n", 7, 0) == -1) {
            LOG_EMERG("NetworkTask", "Error: send()", strerror(errno));
            break;
          }
          */
        } else {
          LOG_TRACE("NetworkTask", "GET: [%s]", s.ToString().c_str());
          std::string msg = "NOT_FOUND\r\n";
          if (send(sockfd_, msg.c_str(), msg.length(), 0) == -1) {
            LOG_EMERG("NetworkTask", "Error: send() - %s", strerror(errno));
            break;
          }
        }
        is_new = true;
        is_new_buffer = true;
        delete value;
        delete buffer;
      } else {
        LOG_EMERG("NetworkTask", "Could not match Get command");
        break;
      }
    } else if (is_command_remove) {
      std::smatch matches;
      std::string str_buffer = buffer->ToString();
      if (std::regex_search(str_buffer, matches, regex_remove)) {
        buffer->SetOffset(7, buffer->size() - 7 - 2);
        Status s = db_->Remove(buffer);
        if (s.IsOK()) {
          // TODO: check for [noreply], which may be present (see Memcached
          // protocol specs)
          LOG_TRACE("NetworkTask", "REMOVE: ok");
          if (send(sockfd_, "DELETED\r\n", 9, 0) == -1) {
            LOG_EMERG("NetworkTask", "Error - send() %s", strerror(errno));
            break;
          }
        } else {
          LOG_EMERG("NetworkTask", "Remove() error: [%s]", s.ToString().c_str());
          break;
        }
        is_new = true;
        is_new_buffer = true;
      } else {
        LOG_EMERG("NetworkTask", "Could not match Remove command");
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
        ByteArray *key_current = new SharedAllocatedByteArray(key->size());
        memcpy(key_current->data(), key->data(), key->size());
        LOG_TRACE("NetworkTask", "call PutChunk key [%s] bytes_received_buffer:%llu bytes_received_total:%llu bytes_expected:%llu size_chunk:%llu", key->ToString().c_str(), bytes_received_buffer, bytes_received_total, bytes_expected, chunk->size());
        Status s = db_->PutChunk(key_current,
                                 chunk,
                                 offset_chunk,
                                 size_value);
        if (!s.IsOK()) {
          LOG_TRACE("NetworkTask", "Error - Put(): %s", s.ToString().c_str());
        } else {
          buffer = nullptr;
        }
      }

      if (bytes_received_total == bytes_expected) {
        is_new = true;
        LOG_TRACE("NetworkTask", "STORED key [%s] bytes_received_buffer:%llu bytes_received_total:%llu bytes_expected:%llu", key->ToString().c_str(), bytes_received_buffer, bytes_received_total, bytes_expected);
        if (send(sockfd_, "STORED\r\n", 8, 0) == -1) {
          LOG_EMERG("NetworkTask", "Error - send() %s", strerror(errno));
          break;
        }

      }
      is_new_buffer = true;
    } else {
      // for debugging
      LOG_EMERG("NetworkTask", "Unknown case for buffer");
      exit(-1);
    }
  }
  LOG_TRACE("NetworkTask", "exit and close socket");

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


Status Server::Start(std::string dbname, int port, int backlog, int num_threads) {

  // Ignoring SIGPIPE, which would crash the program when writing to
  // a broken socket -- doing this because MSG_NOSIGNAL doesn't work on Mac OS X
  signal(SIGPIPE, SIG_IGN);

  struct addrinfo ai_hints, *ai_server, *ai_ptr;
  memset(&ai_hints, 0, sizeof(ai_hints));
  ai_hints.ai_family = AF_UNSPEC;
  ai_hints.ai_socktype = SOCK_STREAM;
  ai_hints.ai_flags = AI_PASSIVE;
  std::string str_port = std::to_string(port);
  int ret;
  if ((ret = getaddrinfo(NULL, str_port.c_str(), &ai_hints, &ai_server)) != 0) {
    return Status::IOError("Server - getaddrinfo", gai_strerror(ret));
  }

  // Bind to the first result
  int sockfd_listen;
  for(ai_ptr = ai_server; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next) {
    if ((sockfd_listen = socket(ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol)) == -1) {
      continue;
    }

    int setsockopt_yes=1;
    if (setsockopt(sockfd_listen, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1) {
      close(sockfd_listen);
      return Status::IOError("Server - setsockopt", strerror(errno));
    }

    if (bind(sockfd_listen, ai_ptr->ai_addr, ai_ptr->ai_addrlen) == -1) {
      close(sockfd_listen);
      continue;
    }

    break;
  }

  if (ai_ptr == NULL) return Status::IOError("Server - Failed to bind");
  freeaddrinfo(ai_server);

  if (listen(sockfd_listen, backlog) == -1) {
    return Status::IOError("Server - listen", strerror(errno));
  }

  // Create the database object and the thread pool
  kdb::KingDB db(dbname);
  ThreadPool tp(num_threads);
  tp.Start();
  LOG_TRACE("Server", "waiting for connections...");

  // Start accepting connections
  int sockfd_accept;
  struct sockaddr_storage sockaddr_client;
  socklen_t size_sa;
  char address[INET6_ADDRSTRLEN];
  while(1) {
    size_sa = sizeof(sockaddr_client);
    sockfd_accept = accept(sockfd_listen, (struct sockaddr *)&sockaddr_client, &size_sa);
    if (sockfd_accept == -1) continue;

    inet_ntop(sockaddr_client.ss_family,
              GetSockaddrIn((struct sockaddr *)&sockaddr_client),
              address,
              sizeof(address));
    LOG_TRACE("Server", "got connection from %s\n", address);

    tp.AddTask(new NetworkTask(sockfd_accept, &db));
  }

  return Status::OK();
}

}
