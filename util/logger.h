// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_LOGGER_H_
#define KINGDB_LOGGER_H_

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
      set_current_level(EMERG);
    } else if (l == "alert") {
      set_current_level(ALERT);
    } else if (l == "crit") {
      set_current_level(CRIT);
    } else if (l == "error") {
      set_current_level(ERROR);
    } else if (l == "warn") {
      set_current_level(WARN);
    } else if (l == "notice") {
      set_current_level(NOTICE);
    } else if (l == "info") {
      set_current_level(INFO);
    } else if (l == "debug") {
      set_current_level(DEBUG);
    } else if (l == "trace") {
      set_current_level(TRACE);
    } else {
      return -1;
    }
    return 0;
  }

  enum Loglevel {
    EMERG=0,
    ALERT=1,
    CRIT=2,
    ERROR=3,
    WARN=4,
    NOTICE=5,
    INFO=6,
    DEBUG=7,
    TRACE=8
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
    Logger::Logv(false, Logger::EMERG, logname, format, args);
    va_end(args);
  }

  static void alert(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::ALERT, logname, format, args);
    va_end(args);
  }

  static void crit(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::CRIT, logname, format, args);
    va_end(args);
  }

  static void error(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::ERROR, logname, format, args);
    va_end(args);
  }

  static void warn(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::WARN, logname, format, args);
    va_end(args);
  }

  static void notice(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::NOTICE, logname, format, args);
    va_end(args);
  }

  static void info(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::INFO, logname, format, args);
    va_end(args);
  }

  static void debug(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::DEBUG, logname, format, args);
    va_end(args);
  }

  static void trace(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::TRACE, logname, format, args);
    va_end(args);
  }

};


#define LOG_EMERG(logname, fmt, ...) Logger::Logv(false, Logger::EMERG, logname, fmt, ##__VA_ARGS__)
#define LOG_ALERT(logname, fmt, ...) Logger::Logv(true, Logger::ALERT, logname, fmt, ##__VA_ARGS__)
#define LOG_CRIT(logname, fmt, ...) Logger::Logv(true, Logger::CRIT, logname, fmt, ##__VA_ARGS__)
#define LOG_ERROR(logname, fmt, ...) Logger::Logv(true, Logger::ERROR, logname, fmt, ##__VA_ARGS__)
#define LOG_WARN(logname, fmt, ...) Logger::Logv(true, Logger::WARN, logname, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(logname, fmt, ...) Logger::Logv(true, Logger::NOTICE, logname, fmt, ##__VA_ARGS__)
#define LOG_INFO(logname, fmt, ...) Logger::Logv(true, Logger::INFO, logname, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(logname, fmt, ...) Logger::Logv(true, Logger::DEBUG, logname, fmt, ##__VA_ARGS__)
#define LOG_TRACE(logname, fmt, ...) Logger::Logv(true, Logger::TRACE, logname, fmt, ##__VA_ARGS__)

}

#endif
