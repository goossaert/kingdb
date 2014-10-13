// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include <iostream>
#include <iomanip>
#include <thread>
#include <regex>
#include <queue>
#include <vector>
#include <string>
#include <cstdio>
#include <string.h>
#include <execinfo.h>
#include <chrono>
#include <sstream>

#include <gperftools/profiler.h>

#include "interface/kingdb.h"
#include "kingdb/kdb.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"

#include "interface/snapshot.h"
#include "interface/iterator.h"


void handler(int sig) {
  int depth_max = 20;
  void *array[depth_max];
  size_t depth;

  depth = backtrace(array, depth_max);
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, depth, STDERR_FILENO);
  exit(1);
}

int main() {
  ProfilerStart("/tmp/kingdb.prof");

  signal(SIGSEGV, handler);
  signal(SIGABRT, handler);

  kdb::Logger::set_current_level("trace");

  kdb::DatabaseOptions options;
  kdb::KingDB db(options, "mydb");

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;

  int size = SIZE_LARGE_TEST_ITEMS;
  char *buffer_large = new char[size+1];
  for (auto i = 0; i < size; i++) {
    buffer_large[i] = 'a';
  }
  buffer_large[size] = '\0';

  int num_items = 10;
  std::vector<std::string> items;
  int size_key = 16;
  
  for (auto i = 0; i < num_items; i++) {
    std::stringstream ss;
    ss << std::setfill ('0') << std::setw (size_key);
    ss << i;
    //std::cout << ss.str() << std::endl;
    items.push_back(ss.str());
  }

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
  for (auto i = 0; i < num_items; i++) {
    kdb::ByteArray *key = new kdb::SimpleByteArray(items[i].c_str(), items[i].size());
    kdb::ByteArray *value = new kdb::SimpleByteArray(buffer_large, 100);
    kdb::Status s = db.PutChunk(write_options,
                                key,
                                value,
                                0,
                                100);
  }

  std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "done in " << duration << " ms" << std::endl;

  kdb::Interface *snapshot = db.NewSnapshot();
  kdb::Iterator *iterator = snapshot->NewIterator(read_options);

  auto count_items = 0;
  for (iterator->Begin(); iterator->IsValid(); iterator->Next()) {
    //std::cout << "key: " << iterator->GetKey()->ToString() << std::endl;
    //std::cout << "value: ";

    kdb::ByteArray *value = iterator->GetValue();
    char *chunk;
    uint64_t size_chunk;
    kdb::Status s;
    while (true) {
      s = value->data_chunk(&chunk, &size_chunk);
      if (s.IsDone()) break;
      if (!s.IsOK()) {
        delete[] chunk;
        fprintf(stderr, "ClientEmbedded - Error - data_chunk(): %s", s.ToString().c_str());
        break;
      }
      //std::cout << std::string(chunk, size_chunk);
      delete[] chunk;
    }
    //std::cout << std::endl;
    //std::cout << std::endl;
    count_items += 1;
  }

  delete iterator;
  delete snapshot;

  std::cout << "count items: " << count_items << std::endl;
  delete[] buffer_large;
  ProfilerStop();
  ProfilerFlush();
  return 0;
}
