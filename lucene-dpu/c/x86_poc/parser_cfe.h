/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __PARSER_CFE_H__
#define __PARSER_CFE_H__

#include "parser_index_header.h"
#include "parser_codec_footer.h"

#include <stdint.h>
#include <stdio.h>

typedef struct {
    uint8_t *entry_name;
    uint64_t offset;
    uint64_t length;
} lucene_cfe_entries_t;

typedef struct {
    lucene_index_header_t *index_header;
    int32_t NumEntries;
    lucene_cfe_entries_t *entries;
    lucene_codec_footer_t *codec_footer;
} lucene_cfe_file_t;

lucene_cfe_file_t *parse_cfe_file(FILE *f_cfe);

void print_cfe_file(lucene_cfe_file_t *cfe_file);

void free_cfe_file(lucene_cfe_file_t *cfe_file);

#endif /* __PARSER_CFE_H__ */
