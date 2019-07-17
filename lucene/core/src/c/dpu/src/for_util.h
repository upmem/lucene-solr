/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __FOR_UTIL_H__
#define __FOR_UTIL_H__

#include <stdint.h>

typedef struct {
    uint32_t bits_per_value;
    uint32_t long_block_count;
    uint32_t long_value_count;
    uint32_t byte_block_count;
    uint32_t byte_value_count;
    uint64_t mask;
    uint32_t int_mask;
} packed_int_decoder_t;

#include "for_util_struct.h"

#endif /* __FOR_UTIL_H__ */
