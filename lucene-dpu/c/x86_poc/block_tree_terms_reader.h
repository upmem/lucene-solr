/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_BLOCK_TREE_TERMS_READER_H
#define X86_POC_BLOCK_TREE_TERMS_READER_H

#include "data_input.h"
#include "fst.h"

typedef struct _block_tree_term_reader_t block_tree_term_reader_t;

typedef struct {
    fst_t* index;
    uint32_t longs_size;
    block_tree_term_reader_t* parent;
} field_reader_t;

typedef struct {
    uint32_t name;
    field_reader_t* field_reader;
} block_tree_term_reader_field_t;

struct _block_tree_term_reader_t {
    uint32_t nr_fields;
    block_tree_term_reader_field_t* fields;
    data_input_t* terms_in;
};


field_reader_t* field_reader_new(block_tree_term_reader_t* parent, uint64_t index_start_fp, uint32_t longs_size, data_input_t* index_in);
block_tree_term_reader_t* block_tree_term_reader_new(data_input_t* terms_in, uint32_t terms_in_length, data_input_t* input_in, uint32_t input_in_length);
field_reader_t* get_terms(block_tree_term_reader_t* reader, uint32_t field);

#endif //X86_POC_BLOCK_TREE_TERMS_READER_H
