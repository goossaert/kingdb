// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "util/status.h"

namespace kdb {

std::string Status::ToString() const {
  if (message1_ == "") {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOK:
        type = "OK";
        break;
      case kNotFound:
        type = "Not found: ";
        break;
      case kDeleteOrder:
        type = "Delete order: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      case kDone:
        type = "Done: ";
        break;
      case kMultipartRequired:
        type = "MultipartRequired: the entry is too large to fit in memory, use the multipart API instead.";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code (%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    result.append(message1_);
    if (message2_.size() > 0) {
      result.append(" - ");
      result.append(message2_);
    }
    return result;
  }
}

};
