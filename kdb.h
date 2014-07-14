// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_HEADERS_H_
#define KINGDB_HEADERS_H_

#include "logger.h"
#include "status.h"
#include "common.h"
#include "event_manager.h"

/*
#define SIZE_BUFFER_WRITE     1024         // used by the BufferManager
#define SIZE_BUFFER_RECV      1024*256     // used by server to receive commands from clients
#define SIZE_BUFFER_SEND      1024*1024*32 // used by server to prepare data to send to clients
#define SIZE_BUFFER_CLIENT    1024*1024*65 // used by client to get data from server
#define SIZE_LOGFILE_HEADER   1024*8       // padding at top of log files
#define SIZE_LOGFILE_TOTAL    (SIZE_LOGFILE_HEADER + 1024* 2)  // maximum size log files can have for small items
*/

#define SIZE_BUFFER_WRITE     1024*1024*32 // used by the BufferManager
#define SIZE_BUFFER_RECV      1024         // used by server to receive commands from clients
#define SIZE_BUFFER_SEND      1024*1024*32 // used by server to prepare data to send to clients
#define SIZE_BUFFER_CLIENT    1024*1024*65 // used by client to get data from server
#define SIZE_LOGFILE_HEADER   1024*8       // padding at top of log files
#define SIZE_LOGFILE_TOTAL    (SIZE_LOGFILE_HEADER + 1024*1024*32)  // maximum size log files can have for small items

#define SIZE_LARGE_TEST_ITEMS 1024*1024*64 // size of large items used for testing


#endif // KINGDB_HEADERS_H_
