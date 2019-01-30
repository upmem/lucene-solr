/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "parser_codec_footer.h"
#include "allocation.h"

#include <stdio.h>

lucene_codec_footer_t *read_codec_footer(data_input_t *buffer) {
    lucene_codec_footer_t *codec_footer = allocation_get(sizeof(lucene_codec_footer_t));

    codec_footer->Magic_footer = read_int(buffer);
    codec_footer->AlgorithmID = read_int(buffer);
    codec_footer->Checksum = read_long(buffer);

    return codec_footer;
}

void print_codec_footer(lucene_codec_footer_t *codec_footer) {
    printf("MagicFooter: %i\n"
           "AlgorithmID: %i\n"
           "Checksum: %llu\n",
           codec_footer->Magic_footer,
           codec_footer->AlgorithmID,
           codec_footer->Checksum);
}

void free_codec_footer(lucene_codec_footer_t *codec_footer) {
    allocation_free(codec_footer);
}
