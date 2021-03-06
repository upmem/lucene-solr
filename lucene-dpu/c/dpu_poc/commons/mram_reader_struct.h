/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_READER_STRUCT_H
#define DPU_POC_MRAM_READER_STRUCT_H

#include "dpu_ptr.h"

typedef struct {
    mram_addr_t index;
    mram_addr_t base;
    DPU_PTR(mram_cache_t*) cache;
} mram_reader_t;

#endif //DPU_POC_MRAM_READER_STRUCT_H
