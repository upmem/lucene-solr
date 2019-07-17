/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_STRUCTURE_H
#define DPU_POC_MRAM_STRUCTURE_H

#include <stdint.h>
#include "query.h"
#include "dpu_output.h"
#include "dpu_characteristics.h"

#define DMA_ALIGNED(addr) (((addr) + 7) & ~7)

#define MRAM_SIZE (1 << 26)

#define IDF_OUTPUT_OFFSET (0)
#define IDF_OUTPUT_SIZE 16
_Static_assert(sizeof(dpu_idf_output_t) == IDF_OUTPUT_SIZE,
               "check that IDF_OUTPUT_SIZE matches the effective size of dpu_idf_output_t");

#define OUTPUTS_PER_THREAD (64*1024)
#define OUTPUTS_BUFFER_OFFSET (DMA_ALIGNED(IDF_OUTPUT_OFFSET + IDF_OUTPUT_SIZE))
#define OUTPUT_SIZE 16
#define OUTPUTS_BUFFER_SIZE_PER_THREAD (OUTPUTS_PER_THREAD * OUTPUT_SIZE)
#define OUTPUTS_BUFFER_SIZE (OUTPUTS_BUFFER_SIZE_PER_THREAD * NR_THREADS)
_Static_assert(sizeof(dpu_output_t) == OUTPUT_SIZE, "check that OUTPUT_SIZE matches the effective size of dpu_ouput_t");

#define QUERY_BUFFER_OFFSET (DMA_ALIGNED(OUTPUTS_BUFFER_OFFSET + OUTPUTS_BUFFER_SIZE))
#define QUERY_BUFFER_SIZE 24
_Static_assert(QUERY_BUFFER_SIZE == sizeof(query_t),
               "Check that QUERY_BUFFER_SIZE matches the effective size of query_t");

#define SEGMENT_SUMMARY_OFFSET (DMA_ALIGNED((QUERY_BUFFER_OFFSET + QUERY_BUFFER_SIZE)))
#define SEGMENT_SUMMARY_ENTRY_SIZE 8
_Static_assert(SEGMENT_SUMMARY_ENTRY_SIZE == sizeof(uint64_t),
               "Check that SEGMENT_SUMMARY_ENTRY_SIZE matches the effective size used to read it on DPU side");

#define SEGMENTS_OFFSET (DMA_ALIGNED(SEGMENT_SUMMARY_OFFSET + NR_THREADS * SEGMENT_SUMMARY_ENTRY_SIZE))

#endif //DPU_POC_MRAM_STRUCTURE_H
