/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_STRUCTURE_H
#define DPU_POC_MRAM_STRUCTURE_H

#include <stdint.h>

#define MRAM_SIZE (1 << 26)

#define QUERY_BUFFER_OFFSET 0
#define QUERY_BUFFER_SIZE 32

#define SEGMENT_SUMMARY_OFFSET QUERY_BUFFER_OFFSET + QUERY_BUFFER_SIZE
#define SEGMENT_SUMMARY_ENTRY_SIZE 16

#define DMA_ALIGNED(addr) (((addr) + 7) & ~7)

#endif //DPU_POC_MRAM_STRUCTURE_H
