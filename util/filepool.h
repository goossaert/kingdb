// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_FILEPOOL_H_
#define KINGDB_FILEPOOL_H_

#include "util/debug.h"
#include <cinttypes>

#include <list>
#include <map>

// NOTE: The list of unused mmaps is currently a simple std::vector, which is
//       being iterated over whenever a search is done. This is fine for now,
//       and will need to be optimized only when and if needed.

// NOTE: Having a shared mmap for the files can cause issues: if the file is
//       partially written, the filesize will be M. If the file gets more data,
//       the filesize goes higher, to a value of P > M. The shared mmap still
//       assumes a size of M and will fail if used to access data between
//       offsets within [M, P]. For that reason, the file pool keeps a single
//       mmap for every file size encountered, for as long as they are needed.
//       Other options could have been to:
//       1. Use mremap(), but it is not portable.
//       2. Make the file pool not hold an mmap pointer, but a shared pointer
//          to an object that has the shared mmap, and whenever a new size is
//          encountered for a file, release the shared mmap to the previous
//          size. That way, the file pool will only hold the mmap for the
//          latest file size, and the objects using the mmap to the previous
//          sizes would hold these mmaps for as long as they need them.

namespace kdb {

struct FileResource {
  uint32_t fileid;
  int fd;
  uint64_t filesize;
  char* mmap;
  int num_references;
};

class FileManager {

 public:
  FileManager() {
  }

  ~FileManager() {
  }

  Status GetFile(uint32_t fileid, const std::string& filepath, uint64_t filesize, FileResource* file) {
    std::unique_lock<std::mutex> lock(mutex_);
    bool found = false;

    // Look for the mmap in the file descriptor cache
    auto it = files_unused.begin();
    while(it != files_unused.end()) {
      if (it->fileid != fileid) {
        ++it;
      } else if (it->filesize == filesize) {
        break;
      } else {
        // If the current item is for a file with same file id but different
        // filesize (in this case a smaller filesize), then the resource needs
        // to be released.
        munmap(it->mmap, it->filesize);
        close(it->fd);
        it = files_unused.erase(it);
      }
    }

    if (it != files_unused.end()) {
      *file = *it;
      files_unused.erase(it);
      file->num_references = 1;
      files_used.insert(std::pair<uint32_t, FileResource>(fileid, *file));
      found = true;
    } else {
      auto range = files_used.equal_range(fileid);
      auto it = range.first;
      for (; it != range.second; ++it) {
        if (it->second.filesize == filesize) break;
      }

      if (it != range.second) {
        it->second.num_references += 1;
        *file = it->second;
        found = true;
      }
    }
    if (found) return Status::OK();

    // If the file descriptor was not found, a new one is opened
    if (NumFiles() > MaxNumFiles() && !files_unused.empty()) {
      auto it = files_unused.begin();
      munmap(it->mmap, it->filesize);
      close(it->fd);
      files_unused.erase(it);
    }

    int fd = 0;
    if ((fd = open(filepath.c_str(), O_RDONLY)) < 0) {
      log::emerg("FileManager::Mmap()::ctor()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return Status::IOError("Could not open() file");
    }

    log::trace("Mmap::ctor()", "open file: ok");
    char* datafile = static_cast<char*>(mmap(0,
                                             filesize, 
                                             PROT_READ,
                                             MAP_SHARED,
                                             fd,
                                             0));
    if (datafile == MAP_FAILED) {
      log::emerg("Could not mmap() file [%s]: %s", filepath.c_str(), strerror(errno));
      return Status::IOError("Could not mmap() file");
    }

    file->fileid = fileid;
    file->fd = fd;
    file->mmap = datafile;
    file->num_references = 1;
    file->filesize = filesize;
    files_used.insert(std::pair<uint32_t, FileResource>(fileid, *file));

    return Status::OK();
  }


  void ReleaseFile(uint32_t fileid, uint64_t filesize) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto range = files_used.equal_range(fileid);
    auto it = range.first;
    for (; it != range.second; ++it) {
      if (it->second.filesize == filesize) break;
    }
    if (it == range.second) return;

    if (it->second.num_references > 1) {
      it->second.num_references -= 1;
    } else {
      it->second.num_references = 0;
      files_unused.push_back(it->second);
      files_used.erase(it);
    }
  } 


  int NumFiles() {
    return files_unused.size() + files_used.size();
  }

  int MaxNumFiles() {
    return 2048; // TODO: make this is an internal parameter 
  }

 private:
  std::mutex mutex_;
  std::vector<FileResource> files_unused;
  std::multimap<uint32_t, FileResource> files_used;

};

} // namespace kdb

#endif // KINGDB_FILEPOOL_H_
