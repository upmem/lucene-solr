/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERMS_ENUM_STRUCT_H
#define DPU_POC_TERMS_ENUM_STRUCT_H

#include <stdint.h>
#include <stdbool.h>
#include "wram_reader.h"
#include "term_state.h"
#include "term_reader.h"

#define BLOCK_SIZE 128

#define MAX_ARCS_LENGTH 5
#define MAX_STACK_LENGTH 5

typedef struct _terms_enum_t terms_enum_t;

typedef struct _terms_enum_frame_t {
    int32_t ord;

    bool has_terms;
    bool has_terms_orig;
    bool is_floor;

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

    term_state_t state;

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

    wram_reader_t suffixes_reader;
    wram_reader_t floor_data_reader;
    wram_reader_t stats_reader;
    wram_reader_t bytes_reader;
} terms_enum_frame_t;

typedef struct _terms_enum_t {
    mram_reader_t *in;

    terms_enum_frame_t static_frame;
    terms_enum_frame_t *current_frame;

    int32_t target_before_current_length;

    field_reader_t *field_reader;
    mram_reader_t fst_reader;

    arc_t arcs[MAX_ARCS_LENGTH];
    uint32_t arcs_length;
    terms_enum_frame_t stack[MAX_STACK_LENGTH];
    uint32_t stack_length;

    uint32_t valid_index_prefix;

    bytes_ref_t *term;

    bool term_exists;
} terms_enum_t;

#endif //DPU_POC_TERMS_ENUM_STRUCT_H
