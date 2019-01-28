/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __PARSER_SI_H__
#define __PARSER_SI_H__

#include "parser_index_header.h"
#include "parser_codec_footer.h"

#include <stdint.h>
#include <stdio.h>

typedef struct {
    lucene_index_header_t *index_header;
    int32_t SegVersion[3];
    uint8_t HasMinVersion;
    int32_t MinVersion[3];
    int32_t DocCount;
    uint8_t IsCompoundFile;
    int32_t DiagnosticsCount;
    uint8_t ***Diagnostics;
    int32_t FilesCount;
    uint8_t **Files;
    int32_t AttributesCount;
    uint8_t ***Attributes;
    int32_t NumSortFields;
    lucene_codec_footer_t *codec_footer;
} lucene_si_file_t;

lucene_si_file_t *parse_si_file(FILE *f_si);

void print_si_file(lucene_si_file_t *si_file);

void free_si_file(lucene_si_file_t *si_file);

void check_file_in_si(lucene_si_file_t *si_file, char *filename);

#endif /* __PARSER_SI_H__ */
