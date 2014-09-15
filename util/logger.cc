// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "util/logger.h"

namespace kdb {

int Logger::level_ = Logger::INFO;
std::mutex Logger::mutex_;

}
