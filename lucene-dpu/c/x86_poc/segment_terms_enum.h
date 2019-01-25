/*
 * Copyright (c) 2014-2018 - uPmem
 */

#ifndef X86_POC_SEGMENT_TERMS_ENUM_H
#define X86_POC_SEGMENT_TERMS_ENUM_H

#include <stdbool.h>
#include "bytes_ref.h"
#include "data_input.h"
#include "fst.h"

typedef struct {
    fst_t* index;
} field_reader_t;

typedef struct {
    uint32_t doc_freq;
    int64_t total_term_freq;
    uint32_t term_block_ord;
    uint64_t block_file_pointer;
} block_term_state_t;

typedef struct _segment_term_enum_t segment_terms_enum_t;

typedef struct {
    int32_t ord;

    bool has_terms;
    bool has_terms_orig;
    bool is_floor;

    arc_t* arc;

    uint64_t fp;
    uint64_t fp_orig;
    uint64_t fp_end;

    uint32_t prefix;
    uint32_t ent_count;
    int32_t next_ent;

    bool is_last_in_floor;
    bool is_leaf_block;

    int64_t last_sub_fp;

    block_term_state_t* state;

    segment_terms_enum_t* ste;

    uint32_t next_floor_label;
    uint32_t num_follow_floor_blocks;

    uint32_t start_byte_pos;
    uint32_t suffix;
    uint64_t sub_code;

    uint8_t* suffix_bytes;
    uint8_t* floor_data;

    uint32_t floor_data_length;

    data_input_t* floor_data_reader;
    data_input_t* suffixes_reader;
} segment_terms_enum_frame_t;

struct _segment_term_enum_t {
    segment_terms_enum_frame_t* static_frame;
    segment_terms_enum_frame_t* current_frame;

    int32_t target_before_current_length;

    field_reader_t* field_reader;
    data_input_t* fst_reader;

    arc_t* arcs;
    uint32_t arcs_length;
    segment_terms_enum_frame_t* stack;
    uint32_t stack_length;

    uint32_t valid_index_prefix;

    bytes_ref_t* term;

    bool term_exists;
};

field_reader_t* field_reader_new(uint64_t index_start_fp, data_input_t* index_in);
segment_terms_enum_t* segment_terms_enum_new(field_reader_t *field_reader);
bool seek_exact(segment_terms_enum_t* terms_enum, bytes_ref_t* target);

#endif //X86_POC_SEGMENT_TERMS_ENUM_H
