/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_TERM_H
#define X86_POC_TERM_H

#include "bytes_ref.h"

typedef struct {
    char *field;
    bytes_ref_t *bytes;
} term_t;

term_t *term_from_string(char *field, char *bytes);

#endif //X86_POC_TERM_H
