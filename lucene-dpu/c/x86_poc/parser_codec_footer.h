#ifndef __PARSER_CODEC_FOOTER_H__
#define __PARSER_CODEC_FOOTER_H__

#include <stdint.h>
#include "data_input.h"

typedef struct {
    int32_t Magic_footer;
    int32_t AlgorithmID;
    uint64_t Checksum;
} lucene_codec_footer_t;

lucene_codec_footer_t *read_codec_footer(data_input_t *buffer);

void print_codec_footer(lucene_codec_footer_t *codec_footer);

void free_codec_footer(lucene_codec_footer_t *codec_footer);

#endif /* __PARSER_CODEC_FOOTER_H__ */
