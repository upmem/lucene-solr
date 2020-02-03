/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <mram.h>

#include "mram_access.h"
#include "mram_structure.h"

void read_query(query_t *query) { mram_read((__mram_ptr void *)QUERY_BUFFER_OFFSET, query, QUERY_BUFFER_SIZE); }

void fetch_cache_line(uint8_t *cache, uintptr_t mram_addr) { mram_read((__mram_ptr void *)mram_addr, cache, CACHE_SIZE); }
