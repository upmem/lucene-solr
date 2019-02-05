/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "term_state.h"
#include "alloc_wrapper.h"

term_state_t *block_term_state_new(void) {
    term_state_t *result = malloc(sizeof(*result));

    result->last_pos_block_offset = -1;
    result->singleton_doc_id = -1;

    return result;
}