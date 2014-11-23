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

#include "interface/kingdb.h"
#include "kingdb/kdb.h"
#include "util/status.h"
#include "util/order.h"
#include "util/byte_array.h"
#include "util/file.h"

#include "interface/snapshot.h"
#include "interface/iterator.h"

#include "unit-tests/testharness.h"


namespace kdb {

class DBTest {
 public:
  DBTest() {
    dbname_ = "db_test";
    db_ = nullptr;
  }

  void Open() {
    EraseDB();
    db_ = new kdb::KingDB(db_options_, dbname_);
    Status s = db_->Open();
    if (!s.IsOK()) {
      delete db_;
      log::emerg("Server", s.ToString().c_str()); 
    }
  }

  void Close() {
    db_->Close();
    delete db_;
    db_ = nullptr;
    EraseDB();
  }

  void EraseDB() {
    struct dirent *entry;
    DIR *dir;
    char filepath[FileUtil::maximum_path_size()];

    struct stat info;
    if (stat(dbname_.c_str(), &info) != 0) return;

    dir = opendir(dbname_.c_str());
    while ((entry = readdir(dir)) != nullptr) {
      sprintf(filepath, "%s/%s", dbname_.c_str(), entry->d_name);
      std::remove(filepath);
    }
    rmdir(dbname_.c_str());
  }

  kdb::Status Get(const std::string& key, std::string *value_out) {
    return Status::OK();
  }

  kdb::Status Put(const std::string& key, const std::string& value) {
    return Status::OK();
  }

  kdb::KingDB* db_;

 private:
  std::string dbname_;
  DatabaseOptions db_options_;
};



TEST(DBTest, SingleThreadSmallItems) {
  Open();
  kdb::Logger::set_current_level("warn");

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;

  int size = 101;
  char *buffer_large = new char[size+1];
  for (auto i = 0; i < size; i++) {
    buffer_large[i] = 'a';
  }
  buffer_large[size] = '\0';

  int num_items = 1000;
  std::vector<std::string> items;
  int size_key = 16;
  
  for (auto i = 0; i < num_items; i++) {
    std::stringstream ss;
    ss << std::setfill ('0') << std::setw (size_key);
    ss << i;
    //std::cout << ss.str() << std::endl;
    items.push_back(ss.str());
  }

  for (auto i = 0; i < num_items; i++) {
    kdb::ByteArray *key = new kdb::SimpleByteArray(items[i].c_str(), items[i].size());
    kdb::ByteArray *value = new kdb::SimpleByteArray(buffer_large, 100);
    kdb::Status s = db_->PutChunk(write_options,
                                  key,
                                  value,
                                  0,
                                  100);
  }

  int count_items_end = 0;
  kdb::Interface *snapshot = db_->NewSnapshot();
  kdb::Iterator *iterator = snapshot->NewIterator(read_options);

  for (iterator->Begin(); iterator->IsValid(); iterator->Next()) {
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
      delete[] chunk;
    }
    count_items_end += 1;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  delete iterator;
  delete snapshot;
  
  delete[] buffer_large;
  ASSERT_EQ(count_items_end, num_items);
  Close();
}


TEST(DBTest, FileUtil) {
  int fd = open("/tmp/allocate", O_WRONLY|O_CREAT, 0644);
  auto start = std::chrono::high_resolution_clock::now();
  size_t mysize = 1024*1024 * (int64_t)256;
  fprintf(stderr, "mysize: %zu\n", mysize);
  Status s = FileUtil::fallocate(fd, mysize);
  std::cout << s.ToString() << std::endl;
  close(fd);
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<float> duration = end - start;
  std::chrono::milliseconds d = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  std::cout << d.count() << " ms" << std::endl;

  fprintf(stderr, "Free size: %" PRIu64 " GB\n", FileUtil::fs_free_space("/tmp/") / (1024*1024*256));
}



} // end namespace kdb

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

  return kdb::test::RunAllTests();
}
