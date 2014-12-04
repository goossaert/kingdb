// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_LOGGER_H_
#define KINGDB_LOGGER_H_

#include "util/debug.h"

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>
#include <string>
#include <thread>
#include <mutex>
#include <iostream>
#include <iomanip>
#include <cstdarg>
#include <utility>

namespace kdb {

class Logger {
 public:
  Logger(std::string name) { }
  virtual ~Logger() { }

  static void Logv(bool thread_safe, int level, const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logv(thread_safe, level, logname, format, args);
    va_end(args);
  }

  static void Logv(bool thread_safe, int level, const char* logname, const char* format, va_list args) {
    if (level>current_level()) return;
    if (thread_safe) mutex_.lock();
    std::cerr << "[" << std::setw(16) << std::this_thread::get_id() << "] - ";
    std::cerr << logname << " - ";
    vfprintf(stderr, format, args);
    std::cerr << std::endl;
    if (thread_safe) mutex_.unlock();
  }

  static int current_level() { return level_; }
  static void set_current_level(int l) { level_ = l; }
  static int set_current_level(const char* l_in) {
    std::string l(l_in);
    std::transform(l.begin(), l.end(), l.begin(), ::tolower);
    if (l == "emerg") {
      set_current_level(kLogLevelEMERG);
    } else if (l == "alert") {
      set_current_level(kLogLevelALERT);
    } else if (l == "crit") {
      set_current_level(kLogLevelCRIT);
    } else if (l == "error") {
      set_current_level(kLogLevelERROR);
    } else if (l == "warn") {
      set_current_level(kLogLevelWARN);
    } else if (l == "notice") {
      set_current_level(kLogLevelNOTICE);
    } else if (l == "info") {
      set_current_level(kLogLevelINFO);
    } else if (l == "debug") {
      set_current_level(kLogLevelDEBUG);
    } else if (l == "trace") {
      set_current_level(kLogLevelTRACE);
    } else {
      return -1;
    }
    return 0;
  }

  enum Loglevel {
    kLogLevelEMERG=0,
    kLogLevelALERT=1,
    kLogLevelCRIT=2,
    kLogLevelERROR=3,
    kLogLevelWARN=4,
    kLogLevelNOTICE=5,
    kLogLevelINFO=6,
    kLogLevelDEBUG=7,
    kLogLevelTRACE=8
  };

 private:
  static int level_;
  static std::mutex mutex_;
};


class log {
 public:
  static void emerg(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::kLogLevelEMERG, logname, format, args);
    va_end(args);
  }

  static void alert(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelALERT, logname, format, args);
    va_end(args);
  }

  static void crit(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelCRIT, logname, format, args);
    va_end(args);
  }

  static void error(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelERROR, logname, format, args);
    va_end(args);
  }

  static void warn(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelWARN, logname, format, args);
    va_end(args);
  }

  static void notice(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelNOTICE, logname, format, args);
    va_end(args);
  }

  static void info(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelINFO, logname, format, args);
    va_end(args);
  }

  static void debug(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelDEBUG, logname, format, args);
    va_end(args);
  }

  static void trace(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelTRACE, logname, format, args);
    va_end(args);
  }

};


#define LOG_EMERG(logname, fmt, ...) Logger::Logv(false, Logger::kLogLevelEMERG, logname, fmt, ##__VA_ARGS__)
#define LOG_ALERT(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelALERT, logname, fmt, ##__VA_ARGS__)
#define LOG_CRIT(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelCRIT, logname, fmt, ##__VA_ARGS__)
#define LOG_ERROR(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelERROR, logname, fmt, ##__VA_ARGS__)
#define LOG_WARN(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelWARN, logname, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelNOTICE, logname, fmt, ##__VA_ARGS__)
#define LOG_INFO(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelINFO, logname, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelDEBUG, logname, fmt, ##__VA_ARGS__)
#define LOG_TRACE(logname, fmt, ...) Logger::Logv(true, Logger::kLogLevelTRACE, logname, fmt, ##__VA_ARGS__)

}

#endif
