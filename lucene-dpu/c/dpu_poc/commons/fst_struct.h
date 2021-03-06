/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FST_STRUCT_H
#define DPU_POC_FST_STRUCT_H

#include "dpu_ptr.h"

typedef struct {
    uint32_t empty_output_offset;
    DPU_PTR(bytes_ref_t *) empty_output;
    int32_t start_node;
    input_type_t input_type;
    mram_addr_t mram_start_offset;
    uint32_t mram_length;
} flat_fst_t;

#endif //DPU_POC_FST_STRUCT_H
