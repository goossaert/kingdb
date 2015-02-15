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

class KeyGenerator {
 public:
  virtual ~KeyGenerator() {}
  virtual std::string GetKey(uint64_t thread_id, uint64_t index, int size) = 0;
};


class SequentialKeyGenerator: public KeyGenerator {
 public:
  virtual ~SequentialKeyGenerator() {
  }
  virtual std::string GetKey(uint64_t thread_id, uint64_t index, int size) {
    std::stringstream ss;
    ss << std::setfill ('0') << std::setw (size);
    ss << index;
    return ss.str();
  }
};


class RandomKeyGenerator: public KeyGenerator {
 public:
  RandomKeyGenerator() {
    generator = std::mt19937(seq);
    random_dist = std::uniform_int_distribution<int>(0,255);
  }

  virtual ~RandomKeyGenerator() {
  }

  virtual std::string GetKey(uint64_t thread_id, uint64_t index, int size) {
    std::string str;
    str.resize(size);
    for (int i = 0; i < size; i++) {
      str[i] = static_cast<char>(random_dist(generator));
    }
    return str;
  }

 private:
    std::seed_seq seq{1, 2, 3, 4, 5, 6, 7};
    std::mt19937 generator;
    std::uniform_int_distribution<int> random_dist;
};



class DBTest {
 public:
  DBTest() {
    dbname_ = "db_test";
    db_ = nullptr;
    db_options_.compression.type = kLZ4Compression;
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

  kdb::Status Get(ReadOptions& read_options, const std::string& key, std::string *value_out) {
    SimpleByteArray ba_key(key.c_str(), key.size());
    ByteArray* ba_value;
    kdb::Status s = db_->Get(read_options, &ba_key, &ba_value);
    if (s.IsOK()) {
      char buffer[1024];
      int offset = 0;
      for (ba_value->Begin(); ba_value->IsValid(); ba_value->Next()) {
        ByteArray* chunk = ba_value->GetChunk();
        //fprintf(stderr, "db_->Get() - size:%" PRIu64 "\n", chunk->size());
        memcpy(buffer + offset, chunk->data(), chunk->size());
        offset += chunk->size();
        buffer[offset] = '\0';
      }
      *value_out = std::string(buffer);
      delete ba_value;
    }

    return s;
  }

  kdb::Status Put(const std::string& key, const std::string& value) {
    return Status::OK();
  }

  kdb::KingDB* db_;

 private:
  std::string dbname_;
  DatabaseOptions db_options_;
};



TEST(DBTest, KeysWithNullBytes) {
  Open();
  kdb::Status s;
  kdb::Logger::set_current_level("emerg");

  kdb::ReadOptions read_options;
  kdb::WriteOptions write_options;
  write_options.sync = true;
  int num_count_valid = 0;

  std::string keystr1("000000000000key1");
  std::string keystr2("000000000000key2");

  keystr1[5] = '\0';
  keystr2[5] = '\0';

  std::string valuestr1("value1");
  std::string valuestr2("value2");

  kdb::ByteArray *key1, *key2, *value1, *value2, *value_out;
  key1 = new kdb::SimpleByteArray(keystr1.c_str(), keystr1.size());
  value1 = new kdb::SimpleByteArray(valuestr1.c_str(), valuestr1.size());
  s = db_->PutChunk(write_options, key1, value1, 0, value1->size());

  key2 = new kdb::SimpleByteArray(keystr2.c_str(), keystr2.size());
  value2 = new kdb::SimpleByteArray(valuestr2.c_str(), valuestr2.size());
  s = db_->PutChunk(write_options, key2, value2, 0, value2->size());

  key1 = new kdb::SimpleByteArray(keystr1.c_str(), keystr1.size());
  value1 = new kdb::SimpleByteArray(valuestr1.c_str(), valuestr1.size());
  key2 = new kdb::SimpleByteArray(keystr2.c_str(), keystr2.size());
  value2 = new kdb::SimpleByteArray(valuestr2.c_str(), valuestr2.size());

  std::string out_str;

  s = Get(read_options, key1->ToString(), &out_str);
  if (s.IsOK() && out_str == "value1") num_count_valid += 1;
  //fprintf(stderr, "out_str: %s\n", out_str.c_str());

  s = Get(read_options, key2->ToString(), &out_str);
  if (s.IsOK() && out_str == "value2") num_count_valid += 1;
  //fprintf(stderr, "out_str: %s\n", out_str.c_str());

  // Sleeping to let the buffer store the entries on secondary storage
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  s = Get(read_options, key1->ToString(), &out_str);
  if (s.IsOK() && out_str == "value1") num_count_valid += 1;
  //fprintf(stderr, "out_str: %s\n", out_str.c_str());

  s = Get(read_options, key2->ToString(), &out_str);
  if (s.IsOK() && out_str == "value2") num_count_valid += 1;
  //fprintf(stderr, "out_str: %s\n", out_str.c_str());

  delete key1;
  delete key2;
  delete value1;
  delete value2;

  ASSERT_EQ(num_count_valid, 4);
  Close();
}



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

  KeyGenerator* kg = new RandomKeyGenerator();

  for (auto i = 0; i < num_items; i++) {
    //std::string key_str = kg->GetKey(0, i, 16);
    //kdb::ByteArray *key = new kdb::AllocatedByteArray(key_str.c_str(), key_str.size());
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

    s = db_->PutChunk(write_options,
                      key,
                      value,
                      i,
                      total_size);

    write(fd, buffer, size_current);
    //memcpy(buffer_full + i, buffer, size_current);
    if (!s.IsOK()) {
      fprintf(stderr, "PutChunk(): %s\n", s.ToString().c_str());
    }
  }

  close(fd);

  usleep(1 * 1000000);

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

    bytes_read += chunk->size();
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
