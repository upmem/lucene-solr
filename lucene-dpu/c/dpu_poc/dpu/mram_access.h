/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_ACCESS_H
#define DPU_POC_MRAM_ACCESS_H

#include "query.h"
#include <mram.h>

#define MRAM_ACCESS_ALIGNMENT 8
#define NR_FILES_OFFSETS 4
#define CACHE_SIZE_LOG 7
#define CACHE_SIZE 128

void read_query(query_t *query);
void read_files_offsets(uint32_t index, uint32_t* offsets);
void fetch_cache_line(uint8_t* cache, mram_addr_t mram_addr);

#endif //DPU_POC_MRAM_ACCESS_H
