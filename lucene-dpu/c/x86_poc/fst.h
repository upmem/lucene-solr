/*
 * Copyright (c) 2014-2018 - uPmem
 */

#ifndef X86_POC_FST_H
#define X86_POC_FST_H

#include <stdint.h>
#include <stdbool.h>
#include "bytes_ref.h"
#include "data_input.h"

typedef enum {
    INPUT_TYPE_BYTE1,
    INPUT_TYPE_BYTE2,
    INPUT_TYPE_BYTE4
} input_type_t;

typedef struct {
    int32_t label;
    bytes_ref_t* output;
    int64_t target;
    uint8_t flags;
    bytes_ref_t* next_final_output;
    int64_t next_arc;
    int64_t pos_arcs_start;
    int32_t bytes_per_arc;
    int32_t arc_idx;
    int32_t num_arcs;
} arc_t;

typedef struct {
    bytes_ref_t* empty_output;
    int64_t start_node;
    arc_t** cached_root_arcs;
    uint32_t cached_root_arcs_length;
    input_type_t input_type;
} fst_t;

fst_t* fst_new(data_input_t* in);

arc_t* get_first_arc(fst_t* fst, arc_t* arc);
arc_t* find_target_arc(fst_t* fst, int32_t label_to_match, arc_t* follow, arc_t* arc, data_input_t* in);

data_input_t* fst_get_bytes_reader(fst_t* fst);

bool arc_is_final(arc_t* arc);
bool arc_is_last(arc_t* arc);

#endif //X86_POC_FST_H
