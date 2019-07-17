/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FOR_UTIL_STRUCT_H
#define DPU_POC_FOR_UTIL_STRUCT_H

#include <stdint.h>
#include <stdbool.h>

#define FOR_UTIL_ARRAY_LENGTH 33

typedef struct {
    bool setup_done[FOR_UTIL_ARRAY_LENGTH];

    uint32_t encoded_sizes[FOR_UTIL_ARRAY_LENGTH];
    packed_int_decoder_t decoders[FOR_UTIL_ARRAY_LENGTH];
    uint32_t iterations[FOR_UTIL_ARRAY_LENGTH];
} flat_for_util_t;

#endif //DPU_POC_FOR_UTIL_STRUCT_H
