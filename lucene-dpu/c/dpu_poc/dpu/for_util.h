/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __FOR_UTIL_H__
#define __FOR_UTIL_H__

#include <stdint.h>
#include <stdbool.h>
#include "mram_reader.h"

typedef struct {
    uint32_t bits_per_value;
    uint32_t long_block_count;
    uint32_t long_value_count;
    uint32_t byte_block_count;
    uint32_t byte_value_count;
    uint64_t mask;
    uint32_t int_mask;
} packed_int_decoder_t;

typedef struct {
    bool *setup_done;

    uint32_t *encoded_sizes;
    packed_int_decoder_t *decoders;
    uint32_t *iterations;
} for_util_t;


for_util_t *build_for_util(mram_reader_t *doc_reader);

#endif /* __FOR_UTIL_H__ */
