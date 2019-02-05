/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERM_READER_H
#define DPU_POC_TERM_READER_H

#include <stdint.h>
#include "fst.h"
#include "field_info.h"

typedef struct _term_reader_t term_reader_t;
typedef struct _field_reader_t field_reader_t;

struct _field_reader_t {
    field_info_t *field_info;
    fst_t *index;
    uint32_t longs_size;
    uint32_t doc_count;
    uint64_t sum_total_term_freq;
    term_reader_t *parent;
};

typedef struct {
    char *name;
    field_reader_t *field_reader;
} term_reader_field_t;

struct _term_reader_t {
    uint32_t nr_fields;
    term_reader_field_t *fields;
    mram_reader_t *terms_in;
};

field_reader_t *fetch_field_reader(term_reader_t *reader, const char *field);

term_reader_t * term_reader_new(field_infos_t *field_infos, mram_reader_t *terms_in, uint32_t terms_in_length,
        mram_reader_t *input_in, uint32_t input_in_length);

#endif //DPU_POC_TERM_READER_H
