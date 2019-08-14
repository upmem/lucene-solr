/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_CACHE_H
#define DPU_POC_MRAM_CACHE_H

#include "mram_access.h"
#include <stdint.h>

#define CACHE_OFFSET_MASK ((1 << CACHE_SIZE_LOG) - 1)
#define CACHE_ADDR_MASK (~(CACHE_OFFSET_MASK))

typedef struct {
    uint8_t __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) contents[CACHE_SIZE];
    mram_addr_t cached;
} mram_cache_t;

mram_cache_t *mram_cache_for(uint32_t task_id);
uint32_t update_mram_cache(mram_cache_t *cache, mram_addr_t mram_addr);

#endif // DPU_POC_MRAM_CACHE_H
