/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FST_H
#define DPU_POC_FST_H

#include <stdint.h>
#include <stdbool.h>
#include "bytes_ref.h"
#include "mram_reader.h"

typedef enum {
    INPUT_TYPE_BYTE1,
    INPUT_TYPE_BYTE2,
    INPUT_TYPE_BYTE4
} input_type_t;

typedef struct {
    int32_t label;
    bytes_ref_t *output;
    int32_t target;
    uint8_t flags;
    bytes_ref_t *next_final_output;
    int32_t next_arc;
    int32_t pos_arcs_start;
    int32_t bytes_per_arc;
    int32_t arc_idx;
    int32_t num_arcs;
} arc_t;

typedef struct {
    uint8_t *bytes_array;
    uint32_t bytes_array_length;
    bytes_ref_t *empty_output;
    int32_t start_node;
    arc_t **cached_root_arcs;
    uint32_t cached_root_arcs_length;
    input_type_t input_type;

    mram_addr_t mram_start_offset;
    uint32_t mram_length;
} fst_t;

bool arc_is_final(arc_t *arc);
bool arc_is_last(arc_t *arc);
arc_t *get_first_arc(fst_t *fst, arc_t *arc);
arc_t *find_target_arc(fst_t *fst, int32_t label_to_match, arc_t *follow, arc_t *arc, mram_reader_t *in);
void fst_fill(fst_t *fst, mram_reader_t *in);

#endif //DPU_POC_FST_H
