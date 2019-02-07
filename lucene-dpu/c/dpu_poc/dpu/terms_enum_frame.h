/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERMS_ENUM_FRAME_H
#define DPU_POC_TERMS_ENUM_FRAME_H

#include <stdbool.h>
#include <stdint.h>
#include "fst.h"
#include "terms_enum.h"
#include "term_state.h"
#include "wram_reader.h"

typedef enum {
    SEEK_STATUS_END,
    SEEK_STATUS_FOUND,
    SEEK_STATUS_NOT_FOUND
} seek_status_t;

typedef struct _terms_enum_t terms_enum_t;

typedef struct _terms_enum_frame_t {
    int32_t ord;

    bool has_terms;
    bool has_terms_orig;
    bool is_floor;

    arc_t *arc;

    uint32_t fp;
    uint32_t fp_orig;
    uint32_t fp_end;

    uint32_t prefix;
    uint32_t ent_count;
    int32_t next_ent;

    bool is_last_in_floor;
    bool is_leaf_block;

    int32_t last_sub_fp;

    uint32_t metadata_up_to;

    term_state_t *state;

    terms_enum_t *ste;

    uint32_t next_floor_label;
    uint32_t num_follow_floor_blocks;

    uint32_t start_byte_pos;
    uint32_t suffix;
    uint32_t sub_code;

    uint8_t *suffix_bytes;
    uint8_t *floor_data;
    uint8_t *stat_bytes;
    int64_t *longs;
    uint8_t *bytes;

    uint32_t suffix_bytes_length;
    uint32_t floor_data_length;
    uint32_t stat_bytes_length;
    uint32_t longs_length;
    uint32_t bytes_length;

    wram_reader_t *suffixes_reader;
    wram_reader_t *floor_data_reader;
    wram_reader_t *stats_reader;
    wram_reader_t *bytes_reader;
} terms_enum_frame_t;

terms_enum_frame_t *term_enum_frame_new(terms_enum_t *term_enum, int32_t ord);
void term_enum_frame_init(terms_enum_frame_t *frame, terms_enum_t *term_enum, int32_t ord);
void rewind_frame(terms_enum_frame_t *frame);
void set_floor_data(terms_enum_frame_t *frame, wram_reader_t *in, bytes_ref_t *source);
void scan_to_floor_frame(terms_enum_frame_t *frame, bytes_ref_t *target);
void load_next_floor_block(terms_enum_frame_t *frame);
void load_block(terms_enum_frame_t *frame);

seek_status_t scan_to_term(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);

void decode_metadata(terms_enum_frame_t *frame);

#endif //DPU_POC_TERMS_ENUM_FRAME_H
