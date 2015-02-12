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
#include <unistd.h>
#include <execinfo.h>
#include <chrono>
#include <sstream>
#include <csignal>
#include <random>

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
    db_options_.compression.type = kNoCompression;
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



TEST(DBTest, SingleThreadSmallEntries) {
  Open();
  kdb::Logger::set_current_level("emerg");

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;
  write_options.sync = true;

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

  //std::this_thread::sleep_for(std::chrono::milliseconds(10000));

  int count_items_end = 0;
  kdb::Iterator *iterator = db_->NewIterator(read_options);

  for (iterator->Begin(); iterator->IsValid(); iterator->Next()) {
    kdb::ByteArray *value = iterator->GetValue();

    for (value->Begin(); value->IsValid(); value->Next()) {
      kdb::ByteArray *chunk = value->GetChunk();
      chunk->data();
      chunk->size();
      fprintf(stderr, "item %d - chunk\n", count_items_end);
    }

    kdb::Status s = value->GetStatus();
    if (!s.IsOK()) {
      fprintf(stderr, "ClientEmbedded - Error: %s\n", s.ToString().c_str());
    }

    count_items_end += 1;
  }

  //std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  delete iterator;
  
  delete[] buffer_large;
  ASSERT_EQ(count_items_end, num_items);
  //ASSERT_EQ(0,0);
  Close();
}

TEST(DBTest, SingleThreadSingleLargeEntry) {
  Open();
  kdb::Logger::set_current_level("emerg");

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;
  write_options.sync = true;

  uint64_t total_size = (uint64_t)1 << 30;
  //total_size *= 5;
  total_size = 1024*1024 * 2;
  int buffersize = 1024 * 64;
  char buffer[buffersize];
  for (int i = 0; i < buffersize; ++i) {
    buffer[i] = 'a';
  }
  std::string key_str = "myentry";
  kdb::Status s;

  std::seed_seq seq{1, 2, 3, 4, 5, 6, 7};
  std::mt19937 generator(seq);
  std::uniform_int_distribution<int> random_dist(0,255);

  //char buffer_full[total_size];

  //usleep(10 * 1000000);

  int fd = open("/tmp/kingdb-input", O_WRONLY|O_CREAT|O_TRUNC, 0644);
  uint32_t crc32_rotating = 0;

  for (uint64_t i = 0; i < total_size; i += buffersize) {

    for (int j = 0; j < buffersize; ++j) {
      char random_char = static_cast<char>(random_dist(generator));
      buffer[j] = random_char;
    }

    kdb::ByteArray *key = new kdb::AllocatedByteArray(const_cast<char*>(key_str.c_str()), key_str.size());
    int size_current = buffersize;
    if (i + size_current > total_size) {
      size_current = total_size - i;
    }

    char *buffer_temp = new char[size_current];
    memcpy(buffer_temp, buffer, size_current);
    kdb::ByteArray *value = new kdb::SimpleByteArray(buffer_temp, size_current);

    uint32_t crc32_debug = kdb::crc32c::Value(value->data(), value->size());
    fprintf(stderr, "PutChunk - before - crc32_debug:0x%08x\n", crc32_debug);
    s = db_->PutChunk(write_options,
                      key,
                      value,
                      i,
                      total_size);

    crc32_rotating = kdb::crc32c::Extend(crc32_rotating, buffer, size_current);
    write(fd, buffer, size_current);
    //memcpy(buffer_full + i, buffer, size_current);
    if (!s.IsOK()) {
      fprintf(stderr, "PutChunk(): %s\n", s.ToString().c_str());
    }
  }

  close(fd);

  fprintf(stderr, "ClientEmbedded - waiting for the buffers to be persisted\n");
  usleep(1 * 1000000);
  fprintf(stderr, "ClientEmbedded - waiting is done\n");

  kdb::ByteArray *value_out;
  kdb::ByteArray *key = new kdb::SimpleByteArray(key_str.c_str(), key_str.size());
  s = db_->Get(read_options, key, &value_out);
  delete key;
  if (!s.IsOK()) {
    fprintf(stderr, "ClientEmbedded - Error - Get(): %s\n", s.ToString().c_str());
  }

  uint64_t bytes_read = 0;
  int fd_output = open("/tmp/kingdb-output", O_WRONLY|O_CREAT|O_TRUNC, 0644);

  for (value_out->Begin(); value_out->IsValid(); value_out->Next()) {
    kdb::ByteArray *chunk = value_out->GetChunk();
    chunk->data();
    chunk->size();
    if (write(fd_output, chunk->data(), chunk->size()) < 0) {
      fprintf(stderr, "ClientEmbedded - Couldn't write to output file: [%s]\n", strerror(errno));
    }
    fprintf(stderr, "data_out after : %p\n", chunk);

    //PrintHex(chunk->data(), chunk->size());
    
    bytes_read += chunk->size();
    fprintf(stderr, "ClientEmbedded - bytes_read: %" PRIu64 " %p %" PRIu64 "\n", bytes_read, chunk, chunk->size());
  }

  s = value_out->GetStatus();
  if (!s.IsOK()) {
    fprintf(stderr, "ClientEmbedded - Error: %s\n", s.ToString().c_str());
  }
  delete value_out;
  close(fd_output);

  ASSERT_EQ(s.IsOK(), true);
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
