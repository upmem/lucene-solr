/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERMS_ENUM_FRAME_H
#define DPU_POC_TERMS_ENUM_FRAME_H

#include <stdbool.h>
#include <stdint.h>
#include "fst.h"
#include "terms_enum_struct.h"

typedef enum {
    SEEK_STATUS_END,
    SEEK_STATUS_FOUND,
    SEEK_STATUS_NOT_FOUND
} seek_status_t;

void term_enum_frame_init(terms_enum_frame_t *frame, terms_enum_t *term_enum, int32_t ord);
void rewind_frame(terms_enum_frame_t *frame);
void set_floor_data(terms_enum_frame_t *frame, wram_reader_t *in, bytes_ref_t *source);
void scan_to_floor_frame(terms_enum_frame_t *frame, bytes_ref_t *target);
void load_next_floor_block(terms_enum_frame_t *frame);
void load_block(terms_enum_frame_t *frame);

seek_status_t scan_to_term(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);

void decode_metadata(terms_enum_frame_t *frame);

#endif //DPU_POC_TERMS_ENUM_FRAME_H
