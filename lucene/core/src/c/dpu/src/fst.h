/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FST_H
#define DPU_POC_FST_H

#include "bytes_ref.h"
#include "mram_reader.h"
#include <stdbool.h>
#include <stdint.h>

typedef enum { INPUT_TYPE_BYTE1, INPUT_TYPE_BYTE2, INPUT_TYPE_BYTE4 } input_type_t;

#include "fst_struct.h"

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

bool arc_is_final(arc_t *arc);
bool arc_is_last(arc_t *arc);
arc_t *get_first_arc(flat_fst_t *fst, arc_t *arc);
arc_t *find_target_arc(flat_fst_t *fst, int32_t label_to_match, arc_t *follow, arc_t *arc, mram_reader_t *in);

#endif // DPU_POC_FST_H
