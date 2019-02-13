/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_LIB_H
#define DPU_POC_LIB_H

#include <stdint.h>
#include "parser.h"
#include "block_tree_terms_reader.h"
#include "norms.h"
#include "segment_terms_enum.h"

typedef struct {
    file_buffer_t *file_buffers;
    block_tree_term_reader_t *term_reader;
    norms_reader_t *norms_reader;
    field_infos_t *field_infos;

    data_input_t *doc_in;
    for_util_t *for_util;
} lucene_global_context_t;

lucene_global_context_t *fetch_lucene_global_context(char *path, uint32_t segment_id);

#endif //DPU_POC_LIB_H
