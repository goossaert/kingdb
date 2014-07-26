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
#include <chrono>
#include <sstream>

#include <gperftools/profiler.h>

#include "kingdb.h"
#include "kdb.h"
#include "status.h"
#include "common.h"
#include "byte_array.h"



int main() {
  ProfilerStart("/tmp/kingdb.prof");
  kdb::KingDB db("mydb");

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
    kdb::ByteArray *key = new kdb::SimpleByteArray(items[i].c_str(), items[i].size());
    kdb::ByteArray *value = new kdb::SimpleByteArray(buffer_large, 100);
    kdb::Status s = db.PutChunk(key,
                                value,
                                0,
                                100);
  }

  std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "done in " << duration << " ms" << std::endl;
  delete[] buffer_large;
  ProfilerStop();
  ProfilerFlush();
  return 0;
}
