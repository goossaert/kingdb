// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "util/logger.h"

namespace kdb {

bool Logger::is_syslog_open_ = false;
int Logger::level_ = Logger::kLogLevelSILENT;
int Logger::log_target_ = Logger::kLogTargetStderr;
std::string Logger::syslog_ident_ = "kingdb";
std::mutex Logger::mutex_;

}
