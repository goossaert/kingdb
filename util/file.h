// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_FILE_H_
#define KINGDB_FILE_H_

#include <sys/resource.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include "util/status.h"

namespace kdb {

class FileUtil {
 public:

  static void increase_limit_open_files() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
      rl.rlim_cur = OPEN_MAX;
      if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        fprintf(stderr, "Could not increase the limit on open files for this process");
      }
    }
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
      LOG_TRACE("disk_free_space()", "statvfs() error");
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
    char filepath[2048];
    Status s;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      sprintf(filepath, "%s/%s", dirpath, entry->d_name);
      if (   strncmp(entry->d_name, prefix.c_str(), prefix.size()) != 0
          || stat(filepath, &info) != 0
          || !(info.st_mode & S_IFREG)) {
        continue;
      }
      if (std::remove(filepath)) {
        LOG_WARN("remove_files_with_prefix()", "Could not remove file [%s]", filepath);
      }
    }
    closedir(directory);
    return Status::OK();
  }
};

} // namespace kdb

#endif // KINGDB_FILE_H_
