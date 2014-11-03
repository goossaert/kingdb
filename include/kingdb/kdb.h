// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_HEADERS_H_
#define KINGDB_HEADERS_H_

#include "util/logger.h"
#include "util/status.h"
#include "util/order.h"
#include "thread/event_manager.h"

/*
#define SIZE_BUFFER_WRITE     1024         // used by the WriteBuffer
#define SIZE_BUFFER_RECV      1024*256     // used by server to receive commands from clients
#define SIZE_BUFFER_SEND      1024*1024*32 // used by server to prepare data to send to clients
#define SIZE_BUFFER_CLIENT    1024*1024*65 // used by client to get data from server
#define SIZE_LOGFILE_HEADER   1024*8       // padding at top of log files
#define SIZE_LOGFILE_TOTAL    (SIZE_LOGFILE_HEADER + 1024* 2)  // maximum size log files can have for small items
*/

#define SIZE_BUFFER_WRITE     1024*1024*32 // used by the WriteBuffer
#define SIZE_BUFFER_RECV      1024*256      // used by server to receive commands from clients
#define SIZE_BUFFER_SEND      1024*1024*32 // used by server to prepare data to send to clients
#define SIZE_BUFFER_CLIENT    1024*1024*65 // used by client to get data from server
#define SIZE_LOGFILE_HEADER   1024*8       // padding at top of log files
#define SIZE_LOGFILE_TOTAL    (SIZE_LOGFILE_HEADER + 1024*1024*32)  // maximum size log files can have for small items

#define SIZE_LARGE_TEST_ITEMS 1024*1024*64 // size of large items used for testing

#define STREAMING_WRITE_TIMEOUT 60 // in seconds

// TODO: make these db_options_
#define dbo_fs_free_space_reject_orders             (SIZE_LOGFILE_TOTAL * 5)
#define dbo_fs_free_space_threshold                 (2 * 1000 * 1024*1024)
#define dbo_size_compaction_uncompacted_has_space   (1 *   70 * 1024*1024) //1 * 1000 * 1024*1024;
#define dbo_size_compaction_uncompacted_no_space    (1 *   70 * 1024*1024) //     256 * 1024*1024;
#define dbo_fs_free_space_sleep                     (     128 * 1024*1024)


#endif // KINGDB_HEADERS_H_
