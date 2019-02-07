/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_INIT_H
#define DPU_POC_INIT_H

#include <stdbool.h>
#include <stdint.h>
#include "file.h"
#include "terms_enum.h"
#include "query.h"
#include "for_util.h"

typedef struct {
    file_buffer_t file_buffers[LUCENE_FILE_ENUM_LENGTH];
    term_reader_t *term_reader;
    field_infos_t *field_infos;
    mram_reader_t *doc_reader;
    for_util_t *for_util;
} search_context_t;

search_context_t *initialize_context(uint32_t index);
query_t *fetch_query(bool do_init);
terms_enum_t *initialize_terms_enum(uint32_t index);

#endif //DPU_POC_INIT_H
