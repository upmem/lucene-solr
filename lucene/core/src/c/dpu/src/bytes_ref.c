/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "bytes_ref.h"
#include "alloc_wrapper.h"
#include <string.h>

const uint8_t EMPTY_ARRAY[0];

const bytes_ref_t EMPTY_BYTES = {
    .bytes = (uint8_t *)EMPTY_ARRAY,
    .offset = 0,
    .length = 0,
    .capacity = 0,
};

bytes_ref_t *bytes_ref_new(void)
{
    bytes_ref_t *result = malloc(sizeof(*result));
    result->bytes = (uint8_t *)EMPTY_ARRAY;
    result->offset = 0;
    result->length = 0;
    result->capacity = 0;
    return result;
}

bytes_ref_t *bytes_ref_from_string(char *string)
{
    bytes_ref_t *result = malloc(sizeof(*result));
    uint32_t length = (uint32_t)strlen(string);

    result->bytes = (uint8_t *)string;
    result->length = length;
    result->capacity = length;
    result->offset = 0;

    return result;
}

bytes_ref_t *bytes_ref_add(bytes_ref_t *prefix, bytes_ref_t *output)
{
    if (prefix == &EMPTY_BYTES) {
        return output;
    }

    if (output == &EMPTY_BYTES) {
        return prefix;
    }

    bytes_ref_t *result = malloc(sizeof(*result));
    result->bytes = malloc(prefix->length + output->length);
    memcpy(result->bytes, prefix->bytes + prefix->offset, prefix->length);
    memcpy(result->bytes + prefix->offset, output->bytes + output->offset, output->length);
    result->offset = 0;
    result->length = prefix->length + output->length;
    result->capacity = prefix->length + output->length;

    return result;
}

void bytes_ref_grow(bytes_ref_t *bytes_ref, uint32_t capacity)
{
    if (bytes_ref->capacity < capacity) {
        uint8_t *array = malloc(capacity);
        memcpy(array, bytes_ref->bytes, bytes_ref->capacity);
        bytes_ref->bytes = array;
        bytes_ref->capacity = capacity;
    }
}