/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "mram_cache.h"
#include "dpu_characteristics.h"

mram_cache_t caches[NR_THREADS];

uint32_t update_mram_cache(mram_cache_t *cache, mram_addr_t mram_addr)
{
    mram_addr_t line_start_addr = mram_addr & CACHE_ADDR_MASK;
    if (line_start_addr != cache->cached) {
        fetch_cache_line(cache->contents, line_start_addr);
        cache->cached = line_start_addr;
    }

    return mram_addr & CACHE_OFFSET_MASK;
}

mram_cache_t *mram_cache_for(uint32_t task_id) { return caches + task_id; }