/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_ACCESS_H
#define DPU_POC_MRAM_ACCESS_H

#include "query.h"
#include <mram.h>

#define MRAM_ACCESS_ALIGNMENT 8
#define CACHE_SIZE_LOG 7
#define CACHE_SIZE 128

#define _CONCAT(x, y) x##y
#define MRAM_READ(to, from, length) _CONCAT(mram_read, length)(from, to)
#define MRAM_WRITE(from, to, length) _CONCAT(mram_write, length)(to, from)

void read_query(query_t *query);
void fetch_cache_line(uint8_t *cache, mram_addr_t mram_addr);

#endif // DPU_POC_MRAM_ACCESS_H
