/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "term.h"
#include "alloc_wrapper.h"

term_t *term_from_string(char *field, char *value) {
    term_t *result = malloc(sizeof(*result));

    result->field = field;
    result->bytes = bytes_ref_from_string(value);

    return result;
}
