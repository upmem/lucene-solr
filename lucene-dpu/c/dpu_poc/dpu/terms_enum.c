/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stddef.h>
#include <string.h>

#include "mram_reader.h"
#include "terms_enum.h"
#include "fst.h"
#include "math.h"
#include "alloc_wrapper.h"
#include "bytes_ref.h"
#include "terms_enum_frame.h"

#define OUTPUT_FLAGS_NUM_BITS 2
#define OUTPUT_FLAG_IS_FLOOR 0x1
#define OUTPUT_FLAG_HAS_TERMS 0x2

static terms_enum_frame_t *push_frame_bytes_ref(terms_enum_t *terms_enum, bytes_ref_t *frame_data, uint32_t length);
static terms_enum_frame_t *get_frame(terms_enum_t *terms_enum, int32_t ord);
static arc_t *get_arc(terms_enum_t *terms_enum, int32_t ord);

bool seek_exact(terms_enum_t *terms_enum, bytes_ref_t *target) {
    arc_t *arc;
    bytes_ref_t *output;
    uint32_t target_up_to;

    bytes_ref_grow(terms_enum->term, 1 + target->length);
    terms_enum->target_before_current_length = terms_enum->current_frame->ord;

    if (terms_enum->current_frame != &terms_enum->static_frame) {
        arc = &terms_enum->arcs[0];
        output = arc->output;
        target_up_to = 0;

        terms_enum_frame_t *last_frame = &terms_enum->stack[0];
        uint32_t target_limit = min(target->length, terms_enum->valid_index_prefix);

        int32_t cmp = 0;

        while (target_up_to < target_limit) {
            cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) -
                  (target->bytes[target->offset + target_up_to] & 0xFF);

            if (cmp != 0) {
                break;
            }

            arc = &terms_enum->arcs[1 + target_up_to];

            if (arc->output != &EMPTY_BYTES) {
                output = bytes_ref_add(output, arc->output);
            }
            if (arc_is_final(arc)) {
                last_frame = &terms_enum->stack[1 + last_frame->ord];
            }
            target_up_to++;
        }

        if (cmp == 0) {
            uint32_t target_up_to_mid = target_up_to;

            uint32_t target_limit_2 = min(target->length, terms_enum->term->length);

            while (target_up_to < target_limit_2) {
                cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) -
                      (target->bytes[target->offset + target_up_to] & 0xFF);

                if (cmp != 0) {
                    break;
                }
                target_up_to++;
            }

            if (cmp == 0) {
                cmp = terms_enum->term->length - target->length;
            }

            target_up_to = target_up_to_mid;
        }

        if (cmp < 0) {
            terms_enum->current_frame = last_frame;
        } else if (cmp > 0) {
            terms_enum->target_before_current_length = last_frame->ord;
            terms_enum->current_frame = last_frame;
            rewind_frame(terms_enum->current_frame);
        } else {
            if (terms_enum->term_exists) {
                return true;
            }
        }
    } else {
        terms_enum->target_before_current_length = -1;
        arc = get_first_arc(&terms_enum->field_reader->index, &terms_enum->arcs[0]);
        output = arc->output;
        terms_enum->current_frame = &terms_enum->static_frame;
        target_up_to = 0;
        terms_enum->current_frame = push_frame_bytes_ref(terms_enum, bytes_ref_add(output, arc->next_final_output),
                                                         0);
    }

    while (target_up_to < target->length) {
        uint32_t target_label = (uint32_t) (target->bytes[target->offset + target_up_to] & 0xFF);

        arc_t *next_arc = find_target_arc(&terms_enum->field_reader->index, target_label, arc,
                                          get_arc(terms_enum, 1 + target_up_to), &terms_enum->fst_reader);

        if (next_arc == NULL) {
            terms_enum->valid_index_prefix = terms_enum->current_frame->prefix;
            scan_to_floor_frame(terms_enum->current_frame, target);

            if (!terms_enum->current_frame->has_terms) {
                terms_enum->term_exists = false;
                terms_enum->term->bytes[target_up_to] = (uint8_t) target_label;
                terms_enum->term->length = 1 + target_up_to;
                return false;
            }

            load_block(terms_enum->current_frame);
            seek_status_t result = scan_to_term(terms_enum->current_frame, target, true);

            if (result == SEEK_STATUS_FOUND) {
                return true;
            } else {
                return false;
            }
        } else {
            arc = next_arc;
            terms_enum->term->bytes[target_up_to] = (uint8_t) target_label;
            if (arc->output != &EMPTY_BYTES) {
                output = bytes_ref_add(output, arc->output);
            }

            target_up_to++;

            if (arc_is_final(arc)) {
                terms_enum->current_frame = push_frame_bytes_ref(terms_enum,
                                                                 bytes_ref_add(output, arc->next_final_output),
                                                                 target_up_to);
            }
        }
    }

    terms_enum->valid_index_prefix = terms_enum->current_frame->prefix;
    scan_to_floor_frame(terms_enum->current_frame, target);

    if (!terms_enum->current_frame->has_terms) {
        terms_enum->term_exists = false;
        terms_enum->term->length = target_up_to;
        return false;
    }

    load_block(terms_enum->current_frame);
    seek_status_t result = scan_to_term(terms_enum->current_frame, target, true);

    if (result == SEEK_STATUS_FOUND) {
        return true;
    } else {
        return false;
    }
}

void init_index_input(terms_enum_t *terms_enum) {
    if (!terms_enum->in_initialized) {
        terms_enum->in_initialized = true;
        mram_reader_fill(&terms_enum->in, &terms_enum->field_reader->parent->terms_in);
    }
}

terms_enum_frame_t *push_frame_fp(terms_enum_t *terms_enum, uint32_t fp, uint32_t length) {
    terms_enum_frame_t *f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);

    if ((f->fp_orig == fp) && (f->next_ent != -1)) {
        if (f->ord > terms_enum->target_before_current_length) {
            rewind_frame(f);
        }
    } else {
        f->next_ent = -1;
        f->prefix = length;
        f->state.term_block_ord = 0;
        f->fp_orig = f->fp = fp;
        f->last_sub_fp = -1;
    }

    return f;
}

static terms_enum_frame_t *push_frame_bytes_ref(terms_enum_t *terms_enum, bytes_ref_t *frame_data, uint32_t length) {
    wram_reader_t reader = {
            .buffer = frame_data->bytes,
            .index = frame_data->offset
    };

    uint32_t code = wram_read_false_vlong(&reader);
    uint32_t fp_seek = code >> OUTPUT_FLAGS_NUM_BITS;
    terms_enum_frame_t *f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);
    f->has_terms = (code & OUTPUT_FLAG_HAS_TERMS) != 0;
    f->has_terms_orig = f->has_terms;
    f->is_floor = (code & OUTPUT_FLAG_IS_FLOOR) != 0;

    if (f->is_floor) {
        set_floor_data(f, &reader, frame_data);
    }

    push_frame_fp(terms_enum, fp_seek, length);

    return f;
}

static terms_enum_frame_t *get_frame(terms_enum_t *terms_enum, int32_t ord) {
    if (ord >= terms_enum->stack_length) {
        if (ord >= MAX_STACK_LENGTH) {
            halt();
        }
        for (int stack_ord = terms_enum->stack_length; stack_ord < ord + 1; ++stack_ord) {
            term_enum_frame_init(terms_enum->stack + stack_ord, terms_enum, stack_ord);
        }
        terms_enum->stack_length = (uint32_t) (ord + 1);
    }

    return terms_enum->stack + ord;
}

static arc_t *get_arc(terms_enum_t *terms_enum, int32_t ord) {
    if (ord >= terms_enum->arcs_length) {
        if (ord >= MAX_ARCS_LENGTH) {
            halt();
        }
        terms_enum->arcs_length = (uint32_t) (ord + 1);
    }

    return terms_enum->arcs + ord;
}

void get_term_state(terms_enum_t *terms_enum, term_state_t *term_state) {
    decode_metadata(terms_enum->current_frame);
    memcpy(term_state, &terms_enum->current_frame->state, sizeof(*term_state));
}

int32_t get_doc_freq(terms_enum_t *terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state.doc_freq;
}

int64_t get_total_term_freq(terms_enum_t *terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state.total_term_freq;
}
