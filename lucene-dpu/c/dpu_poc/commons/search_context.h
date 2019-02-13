/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_SEARCH_CONTEXT_H
#define DPU_POC_SEARCH_CONTEXT_H

#include "mram_reader_struct.h"
#include "for_util_struct.h"
#include "query.h"
#include "field_reader_struct.h"
#include "norms_struct.h"

typedef struct {
    struct {
        uint32_t nr_fields;
        mram_reader_t terms_in;
    } term_reader;

    struct {
        bool has_freq;
        bool has_prox;
        bool has_payloads;
        bool has_offsets;
        bool has_vectors;
        bool has_norms;
        bool has_doc_values;
        bool has_point_values;
        char soft_deletes_field[MAX_FIELD_SIZE];
    } field_infos;

    mram_reader_t doc_reader;

    flat_for_util_t for_util;

    mram_reader_t norms_data;
    uint32_t nr_norms_entries;

    uint32_t empty_outputs_length;

    DPU_PTR(flat_field_reader_t *) fields;
    DPU_PTR(flat_norms_entry_t *) entries;
} flat_search_context_t;

#endif //DPU_POC_SEARCH_CONTEXT_H
