/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __PARSER_INDEX_HEADER_H__
#define __PARSER_INDEX_HEADER_H__

#include <stdint.h>
#include "data_input.h"

#define LUCENE_INDEX_HEADER_OBJECTID_SIZE (16)

typedef struct {
    uint32_t Magic;
    uint8_t *CodecName;
    uint32_t Version;
    int8_t ObjectID[LUCENE_INDEX_HEADER_OBJECTID_SIZE];
    uint8_t SuffixLength;
    uint8_t *SuffixBytes;
} lucene_index_header_t;

lucene_index_header_t *read_index_header(data_input_t *buffer);

void print_index_header(lucene_index_header_t *index_header);

void free_index_header(lucene_index_header_t *index_header);

uint32_t check_index_header(data_input_t* in);
uint32_t check_header(data_input_t* in);

#endif /* __PARSER_INDEX_HEADER_H__ */
