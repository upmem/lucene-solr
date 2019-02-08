/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stddef.h>
#include <stdlib.h>
#include "fst.h"
#include "alloc_wrapper.h"
#include "index_header.h"

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

static arc_t *_find_target_arc(fst_t *fst, int32_t label_to_match, arc_t *follow, arc_t *arc, mram_reader_t *in, bool use_root_arc_cache);
static arc_t *copy_arc_from(arc_t *arc, arc_t *other);
static int32_t read_label(fst_t *fst, mram_reader_t *in);
static arc_t *read_first_real_target_arc(fst_t *fst, int32_t address, arc_t *arc, mram_reader_t *in);
static arc_t *read_next_real_arc(fst_t *fst, arc_t *arc, mram_reader_t *in);
static bytes_ref_t *read_output(mram_reader_t *in);
static bytes_ref_t *read_final_output(mram_reader_t *in);
static void skip_output(mram_reader_t *in);
static void skip_final_output(mram_reader_t *in);
static void seek_to_next_node(fst_t *fst, mram_reader_t *in);
static void cache_root_arcs(fst_t *fst);

static inline bool flag_is_set(uint8_t flags, uint8_t bit) {
    return (flags & bit) != 0;
}

bool arc_is_final(arc_t *arc) {
    return flag_is_set(arc->flags, BIT_FINAL_ARC);
}

bool arc_is_last(arc_t *arc) {
    return flag_is_set(arc->flags, BIT_LAST_ARC);
}

arc_t *get_first_arc(fst_t *fst, arc_t *arc) {
    bytes_ref_t *NO_OUTPUT = (bytes_ref_t *) &EMPTY_BYTES;

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

arc_t *find_target_arc(fst_t *fst, int32_t label_to_match, arc_t *follow, arc_t *arc, mram_reader_t *in) {
    return _find_target_arc(fst, label_to_match, follow, arc, in, true);
}

void fst_fill(fst_t *fst, mram_reader_t *in) {
    check_header(in);

    if (mram_read_byte(in, false) == 1) {
        int32_t num_bytes = mram_read_vint(in, false);

        if (num_bytes <= 0) {
            fst->empty_output = (bytes_ref_t *) &EMPTY_BYTES;
        } else {
            mram_skip_bytes(in, num_bytes - 1, false);

            fst->empty_output = read_final_output(in);

            mram_skip_bytes(in, num_bytes + 1, false);
        }
    } else {
        fst->empty_output = NULL;
    }

    uint8_t t = mram_read_byte(in, false);
    switch (t) {
        case 0:
            fst->input_type = INPUT_TYPE_BYTE1;
            break;
        case 1:
            fst->input_type = INPUT_TYPE_BYTE2;
            break;
        case 2:
            fst->input_type = INPUT_TYPE_BYTE4;
            break;
        default:
            abort();
    }
    fst->start_node = mram_read_false_vlong(in, false);
    uint32_t num_bytes = mram_read_false_vlong(in, false);

    fst->mram_start_offset = in->index;
    fst->mram_length = num_bytes;

    mram_skip_bytes(in, num_bytes, false);

    cache_root_arcs(fst);
}

static void cache_root_arcs(fst_t *fst) {
    // todo: currently caching is disabled

    fst->cached_root_arcs = NULL;
    fst->cached_root_arcs_length = 0;
}

static arc_t *copy_arc_from(arc_t *arc, arc_t *other) {
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

static arc_t *_find_target_arc(fst_t *fst, int32_t label_to_match, arc_t *follow, arc_t *arc, mram_reader_t *in, bool use_root_arc_cache) {
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

    if (use_root_arc_cache && (fst->cached_root_arcs != NULL) && (follow->target == fst->start_node) &&
        (label_to_match < fst->cached_root_arcs_length)) {
        arc_t *result = fst->cached_root_arcs[label_to_match];

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

    set_index(in, (uint32_t) follow->target);

    if (mram_read_byte(in, true) == ARCS_AS_FIXED_ARRAY) {
        arc->num_arcs = mram_read_vint(in, true);
        arc->bytes_per_arc = mram_read_vint(in, true);
        arc->pos_arcs_start = in->index - in->base;
        uint32_t low = 0;
        uint32_t high = (uint32_t) (arc->num_arcs - 1);
        while (low <= high) {
            uint32_t mid = (low + high) >> 1;
            set_index(in, (uint32_t) arc->pos_arcs_start);
            mram_skip_bytes(in, arc->bytes_per_arc * mid + 1, true);
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

static int32_t read_label(fst_t *fst, mram_reader_t *in) {
    int32_t v;

    if (fst->input_type == INPUT_TYPE_BYTE1) {
        v = mram_read_byte(in, true) & 0xFF;
    } else if (fst->input_type == INPUT_TYPE_BYTE2) {
        v = mram_read_short(in, true) & 0xFFFF;
    } else {
        v = mram_read_int(in, true);
    }

    return v;
}

static arc_t *read_first_real_target_arc(fst_t *fst, int32_t address, arc_t *arc, mram_reader_t *in) {
    set_index(in, (uint32_t) address);

    if (mram_read_byte(in, true) == ARCS_AS_FIXED_ARRAY) {
        arc->num_arcs = mram_read_vint(in, true);
        arc->bytes_per_arc = mram_read_vint(in, true);
        arc->arc_idx = -1;
        arc->next_arc = arc->pos_arcs_start = in->index - in->base;
    } else {
        arc->next_arc = address;
        arc->bytes_per_arc = 0;
    }

    return read_next_real_arc(fst, arc, in);
}

static arc_t *read_next_real_arc(fst_t *fst, arc_t *arc, mram_reader_t *in) {
    if (arc->bytes_per_arc != 0) {
        arc->arc_idx++;
        set_index(in, (uint32_t) arc->pos_arcs_start);
        mram_skip_bytes(in, (uint32_t) (arc->arc_idx * arc->bytes_per_arc), true);
    } else {
        set_index(in, (uint32_t) arc->next_arc);
    }

    arc->flags = mram_read_byte(in, true);
    arc->label = read_label(fst, in);

    if (flag_is_set(arc->flags, BIT_ARC_HAS_OUTPUT)) {
        arc->output = read_output(in);
    } else {
        arc->output = (bytes_ref_t *) &EMPTY_BYTES;
    }

    if (flag_is_set(arc->flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        arc->next_final_output = read_final_output(in);
    } else {
        arc->next_final_output = (bytes_ref_t *) &EMPTY_BYTES;
    }

    if (flag_is_set(arc->flags, BIT_STOP_NODE)) {
        if (flag_is_set(arc->flags, BIT_FINAL_ARC)) {
            arc->target = FINAL_END_NODE;
        } else {
            arc->target = NON_FINAL_END_NODE;
        }
        arc->next_arc = in->index - in->base;
    } else if (flag_is_set(arc->flags, BIT_TARGET_NEXT)) {
        arc->next_arc = in->index - in->base;
        if (!flag_is_set(arc->flags, BIT_LAST_ARC)) {
            if (arc->bytes_per_arc == 0) {
                seek_to_next_node(fst, in);
            } else {
                set_index(in, (uint32_t) arc->pos_arcs_start);
                mram_skip_bytes(in, (uint32_t) (arc->bytes_per_arc * arc->num_arcs), true);
            }
        }
        arc->target = in->index - in->base;
    } else {
        arc->target = mram_read_vlong(in, true);
        arc->next_arc = in->index - in->base;
    }
    return arc;
}

static bytes_ref_t *read_output(mram_reader_t *in) {
    uint32_t length = mram_read_vint(in, true);

    if (length == 0) {
        return (bytes_ref_t *) &EMPTY_BYTES;
    } else {
        bytes_ref_t *result = malloc(sizeof(*result));
        result->bytes = malloc(length);
        result->offset = 0;
        mram_read_bytes(in, result->bytes, 0, length, true);
        result->length = length;
        return result;
    }
}

static bytes_ref_t *read_final_output(mram_reader_t *in) {
    return read_output(in);
}

static void skip_output(mram_reader_t *in) {
    uint32_t length = mram_read_vint(in, true);

    if (length != 0) {
        mram_skip_bytes(in, length, true);
    }
}

static void skip_final_output(mram_reader_t *in) {
    skip_output(in);
}

static void seek_to_next_node(fst_t *fst, mram_reader_t *in) {
    while (true) {
        uint8_t flags = mram_read_byte(in, true);
        read_label(fst, in);

        if (flag_is_set(flags, BIT_ARC_HAS_OUTPUT)) {
            skip_output(in);
        }

        if (flag_is_set(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
            skip_final_output(in);
        }

        if (!flag_is_set(flags, BIT_STOP_NODE) && !flag_is_set(flags, BIT_TARGET_NEXT)) {
            mram_read_vlong(in, true);
        }

        if (flag_is_set(flags, BIT_LAST_ARC)) {
            return;
        }
    }
}