/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_INDEX_HEADER_H
#define DPU_POC_INDEX_HEADER_H

#include <stdint.h>
#include "mram_reader.h"

#define LUCENE_INDEX_HEADER_OBJECTID_SIZE (16)

typedef struct {
    uint32_t magic;
    uint8_t *codec_name;
    uint32_t version;
    int8_t object_id[LUCENE_INDEX_HEADER_OBJECTID_SIZE];
    uint8_t suffix_length;
    uint8_t *suffix_bytes;
} index_header_t;

void read_index_header(index_header_t *index_header, mram_reader_t *buffer);

uint32_t check_index_header(mram_reader_t *in);

uint32_t check_header(mram_reader_t *in);

#endif //DPU_POC_INDEX_HEADER_H
