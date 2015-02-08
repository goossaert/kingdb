// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "debug.h"

namespace kdb {

void PrintHex(char *buffer, int size) {
  char hex[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  int col = 16;
  int i = 0;
  for (i = 0; i < size; i++) {
    int value1 = buffer[i] & 0x0f;
    int value2 = (buffer[i] & 0xf0) >> 4;
    fprintf(stderr, "%c%c ", hex[value1], hex[value2]);
    if (i && i % col == 0) fprintf(stderr, "\n");
  }
  if (i && i % col != 0) fprintf(stderr, "\n");
}

} // namespace kdb
