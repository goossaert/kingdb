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

namespace kdb {

class Logger {
 public:
  Logger(std::string name) { }
  virtual ~Logger() { }

  // Write an entry to the log file with the specified format.
  static void Logv(int level, const char* logname, const char* format, ...) {
    if (level>current_level()) return;
    va_list args;
    va_start(args, format);
    /*
    */
    mutex_.lock();
    std::cerr << "[" << std::setw(16) << std::this_thread::get_id() << "] - ";
    std::cerr << logname << " - ";
    vfprintf(stderr, format, args);
    std::cerr << std::endl;
    va_end(args);
    mutex_.unlock();
    //fprintf(stderr, format, ap); 
  }

  //static int current_level() { return Logger::DEBUG; }
  static int current_level() { return level; }
  static void set_current_level( int l ) { level = l; }
  static int level;

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
  static std::mutex mutex_;
};


/*
#define LOG_EMERG(logname, fmt, ...) Logger::Logv(Logger::EMERG, logname, fmt, ##__VA_ARGS__)
#define LOG_ALERT(logname, fmt, ...) Logger::Logv(Logger::ALERT, logname, fmt, ##__VA_ARGS__)
#define LOG_CRIT(logname, fmt, ...) Logger::Logv(Logger::CRIT, logname, fmt, ##__VA_ARGS__)
#define LOG_ERROR(logname, fmt, ...) Logger::Logv(Logger::ERROR, logname, fmt, ##__VA_ARGS__)
#define LOG_WARN(logname, fmt, ...) Logger::Logv(Logger::WARN, logname, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(logname, fmt, ...) Logger::Logv(Logger::NOTICE, logname, fmt, ##__VA_ARGS__)
#define LOG_INFO(logname, fmt, ...) Logger::Logv(Logger::INFO, logname, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(logname, fmt, ...) Logger::Logv(Logger::DEBUG, logname, fmt, ##__VA_ARGS__)
#define LOG_TRACE(logname, fmt, ...) Logger::Logv(Logger::TRACE, logname, fmt, ##__VA_ARGS__)
*/
#define LOG_TRACE(logname, fmt, ...)

}

#endif
