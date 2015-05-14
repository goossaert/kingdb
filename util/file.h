// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_FILE_H_
#define KINGDB_FILE_H_

#include "util/debug.h"

#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <memory>

#include "util/status.h"
#include "util/logger.h"

namespace kdb {

class FileUtil {
 public:

  static void increase_limit_open_files() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
      // TODO: linux compatibility
      //rl.rlim_cur = OPEN_MAX;
      rl.rlim_cur = 4096;
      if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        fprintf(stderr, "Could not increase the limit of open files for this process");
      }
    }
  }

  static std::string kingdb_getcwd() {
    char *buffer = nullptr;
    int size = 64;
    do {
      buffer = new char[size];
      if (getcwd(buffer, size) != NULL) {
        break;
      }
      size *= 2;
      delete[] buffer;
    } while(true);
    std::string cwd(buffer);
    delete[] buffer;
    return cwd;
  }

  // NOTE: Pre-allocating disk space is very tricky: fallocate() and
  //       posix_fallocate() are not supported on all platforms, glibc sometimes
  //       overrides posix_fallocate() with no warnings at all, and for some
  //       filesystems that do not support any pre-allocation, the operating
  //       system can fall back on writing zero bytes in the entire file, making
  //       the operation painfully slow.
  //       The code below is what SQLite and glibc are doing to fake
  //       fallocate(), thus I am employing the same approach, not even checking
  //       if a version of fallocate() was already given by the OS. This may not
  //       be the fastest way to pre-allocate files, but at least it won't be
  //       as slow as zeroing entire files.
  static Status fallocate(int fd, int64_t length) {
    // The code below was copied from fcntlSizeHint() in SQLite (public domain),
    // and modified for clarity and to fit KingDB's needs.
    
    /* If the OS does not have posix_fallocate(), fake it. First use
    ** ftruncate() to set the file size, then write a single byte to
    ** the last byte in each block within the extended region. This
    ** is the same technique used by glibc to implement posix_fallocate()
    ** on systems that do not have a real fallocate() system call.
    */
    struct stat buf;
    if (fstat(fd, &buf) != 0) return Status::IOError("kingdb_fallocate() - fstat()", strerror(errno));
    if (buf.st_size >= length) return Status::IOError("kingdb_fallocate()", "buf.st_size >= length");

    const int blocksize = buf.st_blksize;
    if (!blocksize) return Status::IOError("kingdb_fallocate()", "Invalid block size");
    if (ftruncate(fd, length) != 0) return Status::IOError("kingdb_fallocate() - ftruncate()", strerror(errno));

    int num_bytes_written;
    int64_t offset_write = ((buf.st_size + 2 * blocksize - 1) / blocksize) * blocksize - 1;
    do {
      num_bytes_written = 0;
      if (lseek(fd, offset_write, SEEK_SET) == offset_write) {
        num_bytes_written = write(fd, "", 1);
      }
      offset_write += blocksize;
    } while (num_bytes_written == 1 && offset_write < length);
    if (num_bytes_written != 1) return Status::IOError("kingdb_fallocate() - write()", strerror(errno));
    return Status::OK();
  }

  static Status fallocate_filepath(std::string filepath, int64_t length) {
    int fd;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      return Status::IOError("kingdb_fallocate_filepath() - open()", strerror(errno));
    }
    Status s = fallocate(fd, length);
    close(fd);
    return s;
  }


  static int64_t fs_free_space(const char *filepath) {
    struct statvfs stat;
    if (statvfs(filepath, &stat) != 0) {
      log::trace("disk_free_space()", "statvfs() error");
      return -1;
    }

    if (stat.f_frsize) {
      return stat.f_frsize * stat.f_bavail; 
    } else {
      return stat.f_bsize * stat.f_bavail; 
    }
  }

  static Status remove_files_with_prefix(const char *dirpath, const std::string prefix) {
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dirpath)) == NULL) {
      return Status::IOError("Could not open directory", dirpath);
    }
    char filepath[FileUtil::maximum_path_size()];
    Status s;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      int ret = snprintf(filepath, FileUtil::maximum_path_size(), "%s/%s", dirpath, entry->d_name);
      if (ret < 0 || ret >= FileUtil::maximum_path_size()) {
        log::emerg("remove_files_with_prefix()",
                  "Filepath buffer is too small, could not build the filepath string for file [%s]", entry->d_name); 
        continue;
      }
      if (   strncmp(entry->d_name, prefix.c_str(), prefix.size()) != 0
          || stat(filepath, &info) != 0
          || !(info.st_mode & S_IFREG)) {
        continue;
      }
      if (std::remove(filepath)) {
        log::warn("remove_files_with_prefix()", "Could not remove file [%s]", filepath);
      }
    }
    closedir(directory);
    return Status::OK();
  }

  static uint64_t maximum_path_size() {
    return 4096;
  }

  static int sync_file(int fd) {
    int ret;
#ifdef F_FULLFSYNC
    // For Mac OS X
    ret = fcntl(fd, F_FULLFSYNC);
#else
    ret = fdatasync(fd);
#endif // F_FULLFSYNC
    return ret;
  }
};


class Mmap {
 public:
  Mmap()
      : is_valid_(false),
        fd_(0),
        filesize_(0),
        datafile_(nullptr) {
  }

  Mmap(std::string filepath, int64_t filesize)
      : is_valid_(false),
        fd_(0),
        filesize_(filesize),
        datafile_(nullptr),
        filepath_(filepath) {
    Open();
  }

  virtual ~Mmap() {
    Close();
  }

  void Open(std::string& filepath, uint64_t filesize) {
    filepath_ = filepath;
    filesize_ = filesize;
    Open();
  }

  void Open() {
    if ((fd_ = open(filepath_.c_str(), O_RDONLY)) < 0) {
      log::emerg("Mmap()::ctor()", "Could not open file [%s]: %s", filepath_.c_str(), strerror(errno));
      return;
    }

    log::trace("Mmap::ctor()", "open file: ok");

    datafile_ = static_cast<char*>(mmap(0,
                                       filesize_, 
                                       PROT_READ,
                                       MAP_SHARED,
                                       fd_,
                                       0));
    if (datafile_ == MAP_FAILED) {
      log::emerg("Could not mmap() file [%s]: %s", filepath_.c_str(), strerror(errno));
      return;
    }

    is_valid_ = true;
  }

  void Close() {
    if (datafile_ != nullptr) {
      munmap(datafile_, filesize_);
      close(fd_);
      datafile_ = nullptr;
      is_valid_ = false;
      log::debug("Mmap::~Mmap()", "released mmap on file: [%s]", filepath_.c_str());
    }
  }

  char* datafile() { return datafile_; }
  int64_t filesize() { return filesize_; }
  bool is_valid_;
  bool is_valid() { return is_valid_; }

  int fd_;
  int64_t filesize_;
  char *datafile_;

  // For debugging
  std::string filepath_;
  const char* filepath() const { return filepath_.c_str(); }
};




} // namespace kdb

#endif // KINGDB_FILE_H_
