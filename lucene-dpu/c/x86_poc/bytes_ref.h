/*
 * Copyright (c) 2014-2018 - uPmem
 */

#ifndef X86_POC_BYTES_REF_H
#define X86_POC_BYTES_REF_H

#include <stdint.h>

typedef struct {
    uint8_t* bytes;
    uint32_t offset;
    uint32_t length;
} bytes_ref_t;

#endif //X86_POC_BYTES_REF_H
