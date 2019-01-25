/*
 * Copyright (c) 2014-2018 - uPmem
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include "fst.h"
#include "allocation.h"

#define END_LABEL (-1)

#define FINAL_END_NODE (-1)
#define NON_FINAL_END_NODE (0)

#define BIT_FINAL_ARC            (1 << 0)
#define BIT_LAST_ARC             (1 << 1)
#define BIT_TARGET_NEXT          (1 << 2)
#define BIT_STOP_NODE            (1 << 3)
#define BIT_ARC_HAS_OUTPUT       (1 << 4)
#define BIT_ARC_HAS_FINAL_OUTPUT (1 << 5)

#define ARCS_AS_FIXED_ARRAY BIT_ARC_HAS_FINAL_OUTPUT

static inline bool arc_flag(uint8_t flags, uint8_t bit) {
    return (flags & bit) != 0;
}

static inline arc_t* _find_target_arc(fst_t* fst, int32_t label_to_match, arc_t* follow, arc_t* arc, data_input_t* in, bool use_root_arc_cache);
static arc_t* copy_arc_from(arc_t* arc, arc_t* other);
static int32_t read_label(fst_t* fst, data_input_t* in);

static arc_t* read_first_real_target_arc(fst_t* fst, int64_t address, arc_t* arc, data_input_t* in);
static arc_t* read_next_real_arc(fst_t* fst, arc_t* arc, data_input_t* in);

static bytes_ref_t* read_output(data_input_t* in);
static bytes_ref_t* read_final_output(data_input_t* in);
static void skip_output(data_input_t* in);
static void skip_final_output(data_input_t* in);

static void seek_to_next_node(fst_t* fst, data_input_t* in);

static void cache_root_arcs(fst_t* fst);

fst_t* fst_new(data_input_t* in) {
    fst_t* result = allocation_get(sizeof(*result));

    // todo check header

    if (in->read_byte(in) == 1) {
        int32_t num_bytes = read_vint(in);

        if (num_bytes <= 0) {
            result->empty_output = (bytes_ref_t *) &EMPTY_BYTES;
        } else {
            uint8_t* empty_bytes = allocation_get((size_t) num_bytes);

            read_bytes(in, empty_bytes, 0, (uint32_t) num_bytes);

            data_input_t reader = {
                    .buffer = empty_bytes,
                    .index = 0,
                    .read_byte = decremental_read_byte,
                    .skip_bytes = decremental_skip_bytes
            };

            result->empty_output = read_final_output(&reader);
        }
    } else {
        result->empty_output = NULL;
    }

    uint8_t t = in->read_byte(in);
    switch (t) {
        case 0:
            result->input_type = INPUT_TYPE_BYTE1;
            break;
        case 1:
            result->input_type = INPUT_TYPE_BYTE2;
            break;
        case 2:
            result->input_type = INPUT_TYPE_BYTE4;
            break;
        default:
            fprintf(stderr, "invalid input type %d\n", t);
            exit(1);
    }
    result->start_node = read_vlong(in);
    int64_t num_bytes = read_vlong(in);

    result->bytes_array = allocation_get((size_t) num_bytes);
    read_bytes(in, result->bytes_array, 0, (uint32_t) num_bytes);

    cache_root_arcs(result);

    return result;
}

arc_t* get_first_arc(fst_t* fst, arc_t* arc) {
    bytes_ref_t* NO_OUTPUT = (bytes_ref_t *) &EMPTY_BYTES;

    if (fst->empty_output != NULL) {
        arc->flags = BIT_FINAL_ARC | BIT_LAST_ARC;
        arc->next_final_output = fst->empty_output;
        if (fst->empty_output != NO_OUTPUT) {
            arc->flags |= BIT_ARC_HAS_FINAL_OUTPUT;
        }
    } else {
        arc->flags = BIT_LAST_ARC;
        arc->next_final_output = NO_OUTPUT;
    }

    arc->output = NO_OUTPUT;
    arc->target = fst->start_node;

    return arc;
}

arc_t* find_target_arc(fst_t* fst, int32_t label_to_match, arc_t* follow, arc_t* arc, data_input_t* in) {
    return _find_target_arc(fst, label_to_match, follow, arc, in, true);
}

static void cache_root_arcs(fst_t* fst) {
    // todo
}

static inline arc_t* _find_target_arc(fst_t* fst, int32_t label_to_match, arc_t* follow, arc_t* arc, data_input_t* in, bool use_root_arc_cache) {
    if (label_to_match == END_LABEL) {
        if (arc_is_final(follow)) {
            if (follow->target <= 0) {
                arc->flags = BIT_LAST_ARC;
            } else {
                arc->flags = 0;
                arc->next_arc = follow->target;
            }
            arc->output = follow->next_final_output;
            arc->label = END_LABEL;
            return arc;
        } else {
            return NULL;
        }
    }

    if (use_root_arc_cache && (fst->cached_root_arcs != NULL) && (follow->target == fst->start_node) && (label_to_match < fst->cached_root_arcs_length)) {
        arc_t* result = fst->cached_root_arcs[label_to_match];

        if (result == NULL) {
            return NULL;
        } else {
            copy_arc_from(arc, result);
            return arc;
        }
    }

    if (follow->target <= 0) {
        return NULL;
    }

    in->index = (uint32_t) follow->target;

    if (in->read_byte(in) == ARCS_AS_FIXED_ARRAY) {
        arc->num_arcs = read_vint(in);
        arc->bytes_per_arc = read_vint(in);
        arc->pos_arcs_start = in->index;
        uint32_t low = 0;
        uint32_t high = (uint32_t) (arc->num_arcs - 1);
        while (low <= high) {
            uint32_t mid = (low + high) >> 1;
            in->index = (uint32_t) arc->pos_arcs_start;
            in->skip_bytes(in, arc->bytes_per_arc * mid + 1);
            int32_t mid_label = read_label(fst, in);
            int32_t cmp = mid_label - label_to_match;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                arc->arc_idx = mid - 1;
                return read_next_real_arc(fst, arc, in);
            }
        }

        return NULL;
    }

    read_first_real_target_arc(fst, follow->target, arc, in);

    while (true) {
        if (arc->label == label_to_match) {
            return arc;
        } else if (arc->label > label_to_match) {
            return NULL;
        } else if (arc_is_last(arc)) {
            return NULL;
        } else {
            read_next_real_arc(fst, arc, in);
        }
    }
}

bool arc_is_final(arc_t* arc) {
    return arc_flag(arc->flags, BIT_FINAL_ARC);
}

bool arc_is_last(arc_t* arc) {
    return arc_flag(arc->flags, BIT_LAST_ARC);
}

static arc_t* copy_arc_from(arc_t* arc, arc_t* other) {
    arc->label = other->label;
    arc->target = other->target;
    arc->flags = other->flags;
    arc->output = other->output;
    arc->next_final_output = other->next_final_output;
    arc->next_arc = other->next_arc;
    arc->bytes_per_arc = other->bytes_per_arc;

    if (arc->bytes_per_arc != 0) {
        arc->pos_arcs_start = other->pos_arcs_start;
        arc->arc_idx = other->arc_idx;
        arc->num_arcs = other->num_arcs;
    }
    return arc;
}

static int32_t read_label(fst_t* fst, data_input_t* in) {
    int32_t v;

    if (fst->input_type == INPUT_TYPE_BYTE1) {
        v = in->read_byte(in) & 0xFF;
    } else if (fst->input_type == INPUT_TYPE_BYTE2) {
        v = read_short(in) & 0xFFFF;
    } else {
        v = read_int(in);
    }

    return v;
}

static arc_t* read_first_real_target_arc(fst_t* fst, int64_t address, arc_t* arc, data_input_t* in) {
    in->index = (uint32_t) address;

    if (in->read_byte(in) == ARCS_AS_FIXED_ARRAY) {
        arc->num_arcs = read_vint(in);
        arc->bytes_per_arc = read_vint(in);
        arc->arc_idx = -1;
        arc->next_arc = arc->pos_arcs_start = in->index;
    } else {
        arc->next_arc = address;
        arc->bytes_per_arc = 0;
    }

    return read_next_real_arc(fst, arc, in);
}

static arc_t* read_next_real_arc(fst_t* fst, arc_t* arc, data_input_t* in) {
    if (arc->bytes_per_arc != 0) {
        arc->arc_idx++;
        in->index = (uint32_t) arc->pos_arcs_start;
        in->skip_bytes(in, (uint32_t) (arc->arc_idx * arc->bytes_per_arc));
    } else {
        in->index = (uint32_t) arc->next_arc;
    }

    arc->flags = in->read_byte(in);
    arc->label = read_label(fst, in);

    if (arc_flag(arc->flags, BIT_ARC_HAS_OUTPUT)) {
        arc->output = read_output(in);
    } else {
        arc->output = (bytes_ref_t *) &EMPTY_BYTES;
    }

    if (arc_flag(arc->flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        arc->next_final_output = read_final_output(in);
    } else {
        arc->next_final_output = (bytes_ref_t *) &EMPTY_BYTES;
    }

    if (arc_flag(arc->flags, BIT_STOP_NODE)) {
        if (arc_flag(arc->flags, BIT_FINAL_ARC)) {
            arc->target = FINAL_END_NODE;
        } else {
            arc->target = NON_FINAL_END_NODE;
        }
        arc->next_arc = in->index;
    } else if (arc_flag(arc->flags, BIT_TARGET_NEXT)) {
        arc->next_arc = in->index;
        if (!arc_flag(arc->flags, BIT_LAST_ARC)) {
            if (arc->bytes_per_arc == 0) {
                seek_to_next_node(fst, in);
            } else {
                in->index = (uint32_t) arc->pos_arcs_start;
                in->skip_bytes(in, (uint32_t) (arc->bytes_per_arc * arc->num_arcs));
            }
        }
        arc->target = in->index;
    } else {
        arc->target = read_vlong(in);
        arc->next_arc = in->index;
    }
    return arc;
}

static bytes_ref_t* read_output(data_input_t* in) {
    uint32_t length = read_vint(in);

    if (length == 0) {
        return (bytes_ref_t *) &EMPTY_BYTES;
    } else {
        bytes_ref_t* result = allocation_get(sizeof(*result));
        result->bytes = allocation_get(length);
        result->offset = 0;
        read_bytes(in, result->bytes, 0, length);
        result->length = length;
        return result;
    }
}

static bytes_ref_t* read_final_output(data_input_t* in) {
    return read_output(in);
}

static void skip_output(data_input_t* in) {
    uint32_t length = read_vint(in);

    if (length != 0) {
        in->skip_bytes(in, length);
    }
}

static void skip_final_output(data_input_t* in) {
    skip_output(in);
}

static void seek_to_next_node(fst_t* fst, data_input_t* in) {
    while (true) {
        uint8_t flags = in->read_byte(in);
        read_label(fst, in);

        if (arc_flag(flags, BIT_ARC_HAS_OUTPUT)) {
            skip_output(in);
        }

        if (arc_flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
            skip_final_output(in);
        }

        if (!arc_flag(flags, BIT_STOP_NODE) && !arc_flag(flags, BIT_TARGET_NEXT)) {
            read_vlong(in);
        }

        if (arc_flag(flags, BIT_LAST_ARC)) {
            return;
        }
    }
}

data_input_t* fst_get_bytes_reader(fst_t* fst) {
    data_input_t* result = allocation_get(sizeof(*result));

    result->buffer = fst->bytes_array;
    result->index = 0;
    result->read_byte = decremental_read_byte;
    result->skip_bytes = decremental_skip_bytes;

    return result;
}