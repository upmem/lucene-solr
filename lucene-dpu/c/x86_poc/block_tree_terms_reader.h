/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_BLOCK_TREE_TERMS_READER_H
#define X86_POC_BLOCK_TREE_TERMS_READER_H

#include "data_input.h"
#include "fst.h"
#include "field_infos.h"

typedef struct _block_tree_term_reader_t block_tree_term_reader_t;

typedef struct {
    field_info_t *field_info;
    fst_t *index;
    uint32_t longs_size;
    uint32_t doc_count;
    uint64_t sum_total_term_freq;
    block_tree_term_reader_t *parent;
} field_reader_t;

typedef struct {
    char *name;
    field_reader_t *field_reader;
} block_tree_term_reader_field_t;

struct _block_tree_term_reader_t {
    uint32_t nr_fields;
    block_tree_term_reader_field_t *fields;
    data_input_t *terms_in;
};

block_tree_term_reader_t *
block_tree_term_reader_new(field_infos_t *field_infos, data_input_t *terms_in, uint32_t terms_in_length,
                           data_input_t *input_in, uint32_t input_in_length);

field_reader_t *get_terms(block_tree_term_reader_t *reader, const char *field);

#endif //X86_POC_BLOCK_TREE_TERMS_READER_H
