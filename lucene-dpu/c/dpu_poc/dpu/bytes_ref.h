/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_BYTES_REF_H
#define DPU_POC_BYTES_REF_H

#include <stdint.h>

typedef struct {
    uint8_t *bytes;
    uint32_t offset;
    uint32_t length;
    uint32_t capacity;
} bytes_ref_t;

extern const bytes_ref_t EMPTY_BYTES;

bytes_ref_t *bytes_ref_new(void);

bytes_ref_t *bytes_ref_from_string(char *string);

bytes_ref_t *bytes_ref_add(bytes_ref_t *prefix, bytes_ref_t *output);

void bytes_ref_grow(bytes_ref_t *bytes_ref, uint32_t capacity);

#endif // DPU_POC_BYTES_REF_H
