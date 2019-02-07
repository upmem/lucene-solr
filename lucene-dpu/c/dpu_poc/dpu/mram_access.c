/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <mram.h>

#include "mram_access.h"
#include "mram_structure.h"

#define _CONCAT(x, y) x ## y
#define MRAM_READ(to, from, length) _CONCAT(mram_read, length) (from, to)

void read_query(query_t *query) {
    MRAM_READ(query, QUERY_BUFFER_OFFSET, QUERY_BUFFER_SIZE);
}

void read_files_offsets(uint32_t index, uint32_t* offsets) {
    MRAM_READ(offsets, FILES_SUMMARY_BUFFER_OFFSET + index * FILES_SUMMARY_BUFFER_SIZE, FILES_SUMMARY_BUFFER_SIZE);
}

void fetch_cache_line(uint8_t* cache, mram_addr_t mram_addr) {
    MRAM_READ(cache, mram_addr, CACHE_SIZE);
}