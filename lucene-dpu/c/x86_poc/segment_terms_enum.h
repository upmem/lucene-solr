/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_SEGMENT_TERMS_ENUM_H
#define X86_POC_SEGMENT_TERMS_ENUM_H

#include <stdbool.h>
#include "bytes_ref.h"
#include "data_input.h"
#include "fst.h"
#include "block_tree_terms_reader.h"

#define POSTINGS_ENUM_FREQS (1 << 3)
#define POSTINGS_ENUM_POSITIONS (1 << 4)

typedef struct {
    uint32_t doc_freq;
    int64_t total_term_freq;
    uint32_t term_block_ord;
    uint64_t block_file_pointer;

    uint64_t doc_start_fp;
    uint64_t pos_start_fp;
    uint64_t pay_start_fp;
    int32_t singleton_doc_id;
    int64_t last_pos_block_offset;
    int64_t skip_offset;
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

    uint32_t metadata_up_to;

    block_term_state_t* state;

    segment_terms_enum_t* ste;

    uint32_t next_floor_label;
    uint32_t num_follow_floor_blocks;

    uint32_t start_byte_pos;
    uint32_t suffix;
    uint64_t sub_code;

    uint8_t* suffix_bytes;
    uint8_t* floor_data;
    uint8_t* stat_bytes;
    int64_t* longs;
    uint8_t* bytes;

    uint32_t suffix_bytes_length;
    uint32_t floor_data_length;
    uint32_t stat_bytes_length;
    uint32_t longs_length;
    uint32_t bytes_length;

    data_input_t* suffixes_reader;
    data_input_t* floor_data_reader;
    data_input_t* stats_reader;
    data_input_t* bytes_reader;
} segment_terms_enum_frame_t;

struct _segment_term_enum_t {
    data_input_t* in;

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

typedef struct {
    uint32_t bits_per_value;
    uint32_t long_block_count;
    uint32_t long_value_count;
    uint32_t byte_block_count;
    uint32_t byte_value_count;
    uint64_t mask;
    uint32_t int_mask;
} packed_int_decoder_t;

typedef struct {
    bool* setup_done;

    uint32_t* encoded_sizes;
    packed_int_decoder_t* decoders;
    uint32_t* iterations;
} for_util_t;

typedef struct {
    data_input_t* start_doc_in;
    data_input_t* doc_in;

    bool index_has_freq;
    bool index_has_pos;
    bool index_has_offsets;
    bool index_has_payloads;

    uint32_t doc_freq;
    uint64_t total_term_freq;
    uint64_t doc_term_start_fp;
    int64_t skip_offset;
    int32_t singleton_doc_id;

    int32_t doc;
    int32_t freq;
    bool needs_freq;
    int32_t* doc_delta_buffer;
    int32_t* freq_buffer;
    uint32_t accum;
    uint32_t doc_up_to;
    uint32_t next_skip_doc;
    uint32_t doc_buffer_up_to;
    bool skipped;

    uint8_t* encoded;

    for_util_t* for_util;
} postings_enum_t;

segment_terms_enum_t* segment_terms_enum_new(field_reader_t *field_reader);
bool seek_exact(segment_terms_enum_t* terms_enum, bytes_ref_t* target);
block_term_state_t* get_term_state(segment_terms_enum_t* terms_enum);
int32_t get_doc_freq(segment_terms_enum_t* terms_enum);
int64_t get_total_term_freq(segment_terms_enum_t* terms_enum);

postings_enum_t* impacts(segment_terms_enum_t* terms_enum, uint32_t flags, data_input_t* doc_in, for_util_t* for_util);

for_util_t* build_for_util(data_input_t* doc_in);

#endif //X86_POC_SEGMENT_TERMS_ENUM_H
