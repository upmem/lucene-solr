/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <mram.h>

#include "mram_access.h"
#include "mram_structure.h"

void read_query(query_t *query) { MRAM_READ(query, QUERY_BUFFER_OFFSET, QUERY_BUFFER_SIZE); }

void fetch_cache_line(uint8_t *cache, mram_addr_t mram_addr) { MRAM_READ(cache, mram_addr, CACHE_SIZE); }