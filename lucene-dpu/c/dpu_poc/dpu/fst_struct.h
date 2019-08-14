/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FST_STRUCT_H
#define DPU_POC_FST_STRUCT_H

#include "bytes_ref.h"
#include "dpu_ptr.h"
#include <mram.h>

typedef enum { INPUT_TYPE_BYTE1, INPUT_TYPE_BYTE2, INPUT_TYPE_BYTE4 } input_type_t;

typedef struct {
    uint32_t empty_output_offset;
    DPU_PTR(bytes_ref_t *) empty_output;
    int32_t start_node;
    input_type_t input_type;
    mram_addr_t mram_start_offset;
    uint32_t mram_length;
} flat_fst_t;

#endif // DPU_POC_FST_STRUCT_H
