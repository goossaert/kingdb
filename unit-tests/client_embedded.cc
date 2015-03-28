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
#include <csignal>

#include <gperftools/profiler.h>

#include "interface/database.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"

#include "interface/snapshot.h"
#include "interface/iterator.h"


#define SIZE_LARGE_TEST_ITEMS 1024*1024*64 // size of large items used for testing

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

  signal(SIGSEGV, handler);
  signal(SIGABRT, handler);

  kdb::Logger::set_current_level("info");

  kdb::DatabaseOptions options;
  options.compression = kdb::kLZ4Compression;
  //options.storage__maximum_part_size = 128 * 1024;
  kdb::Database db(options, "mydb");
  db.Open();

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;

  int size = SIZE_LARGE_TEST_ITEMS;
  char *buffer_large = new char[size+1];
  for (auto i = 0; i < size; i++) {
    buffer_large[i] = 'a';
  }
  buffer_large[size] = '\0';

  int num_items = 1000000;
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
    kdb::ByteArray key = kdb::NewDeepCopyByteArray(items[i].c_str(), items[i].size());
    kdb::ByteArray value = kdb::NewDeepCopyByteArray(buffer_large, 100);
    kdb::Status s = db.PutPart(write_options,
                                key,
                                value,
                                0,
                                100);
  }

  std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "done in " << duration << " ms" << std::endl;

  kdb::Snapshot snapshot = db.NewSnapshot();
  kdb::Iterator iterator = snapshot.NewIterator(read_options);

#ifdef DEBUG
  ProfilerStart("/tmp/kingdb.prof");
#endif

  iterator.Begin();
  start = std::chrono::high_resolution_clock::now();

  auto count_items = 0;
  for (; iterator.IsValid(); iterator.Next()) {
    kdb::ByteArray key = iterator.GetKey();
    kdb::ByteArray value = iterator.GetValue();
    count_items += 1;
  }

  end = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "iteration done in " << duration << " ms" << std::endl;
  std::cout << "count items: " << count_items << std::endl;
#ifdef DEBUG
  ProfilerStop();
  ProfilerFlush();
#endif
  delete[] buffer_large;
  return 0;
}
