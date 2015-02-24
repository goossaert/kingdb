// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_DEBUG_H_
#define KINGDB_DEBUG_H_

#ifdef DEBUG

// Using valgrind for c++11
#include <valgrind/drd.h>
#define _GLIBCXX_SYNCHRONIZATION_HAPPENS_BEFORE(addr) ANNOTATE_HAPPENS_BEFORE(addr)
#define _GLIBCXX_SYNCHRONIZATION_HAPPENS_AFTER(addr)  ANNOTATE_HAPPENS_AFTER(addr)

#endif // DEBUG

#include <cstdio>


namespace kdb {

void PrintHex(const char* buffer, int size);

} // namespace kdb

#endif // KINGDB_DEBUG_H_
