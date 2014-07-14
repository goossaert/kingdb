// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "logger.h"

namespace kdb {

int Logger::level = Logger::TRACE;
std::mutex Logger::mutex_;

}
