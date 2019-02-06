/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERMS_ENUM_H
#define DPU_POC_TERMS_ENUM_H

#include <stdbool.h>
#include "term_reader.h"
#include "fst.h"
#include "terms_enum_frame.h"
#include "term_state.h"

typedef struct _terms_enum_frame_t terms_enum_frame_t;

typedef struct _terms_enum_t {
    mram_reader_t *in;

    terms_enum_frame_t *static_frame;
    terms_enum_frame_t *current_frame;

    int32_t target_before_current_length;

    field_reader_t *field_reader;
    mram_reader_t *fst_reader;

    arc_t *arcs;
    uint32_t arcs_length;
    terms_enum_frame_t *stack;
    uint32_t stack_length;

    uint32_t valid_index_prefix;

    bytes_ref_t *term;

    bool term_exists;
} terms_enum_t;

bool seek_exact(terms_enum_t *terms_enum, bytes_ref_t *target);
void init_index_input(terms_enum_t *terms_enum);

terms_enum_frame_t *push_frame_fp(terms_enum_t *terms_enum, arc_t *arc, uint64_t fp, uint32_t length);

term_state_t *get_term_state(terms_enum_t *terms_enum);

#endif //DPU_POC_TERMS_ENUM_H
