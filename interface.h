// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_INTERFACE_H_
#define KINGDB_INTERFACE_H_

#include "status.h"
#include "common.h"

namespace kdb {

class Interface {
 public:
  virtual Status Get(const std::string& key, Value **value_out) = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status Remove(const std::string& key) = 0;
};

};

#endif // KINGDB_INTERFACE_H_
