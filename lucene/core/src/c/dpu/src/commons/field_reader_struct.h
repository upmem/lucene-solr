/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FIELD_READER_STRUCT_H
#define DPU_POC_FIELD_READER_STRUCT_H

#include <stdint.h>
#include <stdbool.h>
#include "query.h"
#include "fst_struct.h"

typedef struct {
    char name[MAX_FIELD_SIZE];
    uint32_t number;
    doc_values_type_t doc_values_type;
    bool store_term_vector;
    bool omit_norms;
    index_options_t index_options;
    bool store_payloads;
    // todo attributes
    int64_t dv_gen;
    int32_t point_data_dimension_count;
    int32_t point_index_dimension_count;
    int32_t point_num_bytes;
    bool soft_deletes_field;
} flat_field_info_t;

typedef struct {
    flat_field_info_t field_info;
    flat_fst_t index;
    uint32_t longs_size;
    uint32_t doc_count;
    uint64_t sum_total_term_freq;
} flat_field_reader_t;

#endif //DPU_POC_FIELD_READER_STRUCT_H
