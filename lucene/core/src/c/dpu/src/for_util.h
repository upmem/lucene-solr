/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __FOR_UTIL_H__
#define __FOR_UTIL_H__

#include <stdbool.h>
#include <stdint.h>

#define FOR_UTIL_ARRAY_LENGTH 33

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
    bool setup_done[FOR_UTIL_ARRAY_LENGTH];

    uint32_t encoded_sizes[FOR_UTIL_ARRAY_LENGTH];
    packed_int_decoder_t decoders[FOR_UTIL_ARRAY_LENGTH];
    uint32_t iterations[FOR_UTIL_ARRAY_LENGTH];
} flat_for_util_t;

#endif /* __FOR_UTIL_H__ */
