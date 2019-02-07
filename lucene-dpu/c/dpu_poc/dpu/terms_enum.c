/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stddef.h>
#include <string.h>
#include <devprivate.h>

#include "mram_reader.h"
#include "terms_enum.h"
#include "fst.h"
#include "math.h"
#include "alloc_wrapper.h"
#include "bytes_ref.h"

#define OUTPUT_FLAGS_NUM_BITS 2
#define OUTPUT_FLAG_IS_FLOOR 0x1
#define OUTPUT_FLAG_HAS_TERMS 0x2

static terms_enum_frame_t *push_frame_bytes_ref(terms_enum_t *terms_enum, arc_t *arc, bytes_ref_t *frame_data, uint32_t length);
static terms_enum_frame_t *get_frame(terms_enum_t *terms_enum, int32_t ord);
static arc_t *get_arc(terms_enum_t *terms_enum, int32_t ord);

bool seek_exact(terms_enum_t *terms_enum, bytes_ref_t *target) {
    arc_t *arc;
    bytes_ref_t *output;
    uint32_t target_up_to;

    bytes_ref_grow(terms_enum->term, 1 + target->length);
    terms_enum->target_before_current_length = terms_enum->current_frame->ord;

    tell(terms_enum, "0");

    if (terms_enum->current_frame != terms_enum->static_frame) {
        tell(terms_enum, "1");
        arc = &terms_enum->arcs[0];
        output = arc->output;
        target_up_to = 0;

        terms_enum_frame_t *last_frame = &terms_enum->stack[0];
        uint32_t target_limit = min(target->length, terms_enum->valid_index_prefix);

        int32_t cmp = 0;

        while (target_up_to < target_limit) {
            tell(terms_enum, "2");
            cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) -
                  (target->bytes[target->offset + target_up_to] & 0xFF);

            if (cmp != 0) {
                tell(terms_enum, "3");
                break;
            }

            arc = &terms_enum->arcs[1 + target_up_to];

            if (arc->output != &EMPTY_BYTES) {
                tell(terms_enum, "4");
                output = bytes_ref_add(output, arc->output);
            }
            if (arc_is_final(arc)) {
                tell(terms_enum, "5");
                last_frame = &terms_enum->stack[1 + last_frame->ord];
            }
            target_up_to++;
        }

        tell(terms_enum, "6");
        if (cmp == 0) {
            tell(terms_enum, "7");
            uint32_t target_up_to_mid = target_up_to;

            uint32_t target_limit_2 = min(target->length, terms_enum->term->length);

            while (target_up_to < target_limit_2) {
                tell(terms_enum, "8");
                cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) -
                      (target->bytes[target->offset + target_up_to] & 0xFF);

                if (cmp != 0) {
                    tell(terms_enum, "9");
                    break;
                }
                target_up_to++;
            }

            if (cmp == 0) {
                tell(terms_enum, "10");
                cmp = terms_enum->term->length - target->length;
            }

            target_up_to = target_up_to_mid;
        }

        tell(terms_enum, "11");
        if (cmp < 0) {
            tell(terms_enum, "12");
            terms_enum->current_frame = last_frame;
        } else if (cmp > 0) {
            tell(terms_enum, "13");
            terms_enum->target_before_current_length = last_frame->ord;
            terms_enum->current_frame = last_frame;
            rewind_frame(terms_enum->current_frame);
        } else {
            tell(terms_enum, "14");
            if (terms_enum->term_exists) {
                tell(terms_enum, "15");
                return true;
            }
        }
    } else {
        tell(terms_enum, "16");
        terms_enum->target_before_current_length = -1;
        arc = get_first_arc(terms_enum->field_reader->index, &terms_enum->arcs[0]);
        output = arc->output;
        terms_enum->current_frame = terms_enum->static_frame;
        target_up_to = 0;
        terms_enum->current_frame = push_frame_bytes_ref(terms_enum, arc, bytes_ref_add(output, arc->next_final_output),
                                                         0);
    }

    tell(terms_enum, "17");
    while (target_up_to < target->length) {
        tell(terms_enum, "18");
        uint32_t target_label = (uint32_t) (target->bytes[target->offset + target_up_to] & 0xFF);

        arc_t *next_arc = find_target_arc(terms_enum->field_reader->index, target_label, arc,
                                          get_arc(terms_enum, 1 + target_up_to), terms_enum->fst_reader);

        tell(target_label, "0x1000");
        tell(target_up_to, "0x1001");

        if (next_arc == NULL) {
            tell(terms_enum, "19");
            terms_enum->valid_index_prefix = terms_enum->current_frame->prefix;
            scan_to_floor_frame(terms_enum->current_frame, target);

            if (!terms_enum->current_frame->has_terms) {
                tell(terms_enum, "20");
                terms_enum->term_exists = false;
                terms_enum->term->bytes[target_up_to] = (uint8_t) target_label;
                terms_enum->term->length = 1 + target_up_to;
                return false;
            }

            load_block(terms_enum->current_frame);
            seek_status_t result = scan_to_term(terms_enum->current_frame, target, true);

            tell(terms_enum, "21");
            if (result == SEEK_STATUS_FOUND) {
                tell(terms_enum, "22");
                return true;
            } else {
                tell(terms_enum, "23");
                return false;
            }
        } else {
            tell(terms_enum, "24");
            arc = next_arc;
            terms_enum->term->bytes[target_up_to] = (uint8_t) target_label;
            if (arc->output != &EMPTY_BYTES) {
                tell(terms_enum, "25");
                output = bytes_ref_add(output, arc->output);
            }

            target_up_to++;

            if (arc_is_final(arc)) {
                tell(terms_enum, "26");
                terms_enum->current_frame = push_frame_bytes_ref(terms_enum, arc,
                                                                 bytes_ref_add(output, arc->next_final_output),
                                                                 target_up_to);
            }
        }
    }
    tell(terms_enum, "27");

    terms_enum->valid_index_prefix = terms_enum->current_frame->prefix;
    scan_to_floor_frame(terms_enum->current_frame, target);

    if (!terms_enum->current_frame->has_terms) {
        tell(terms_enum, "28");
        terms_enum->term_exists = false;
        terms_enum->term->length = target_up_to;
        return false;
    }

    load_block(terms_enum->current_frame);
    seek_status_t result = scan_to_term(terms_enum->current_frame, target, true);

    tell(terms_enum, "29");
    if (result == SEEK_STATUS_FOUND) {
        tell(terms_enum, "30");
        return true;
    } else {
        tell(terms_enum, "31");
        return false;
    }
}

void init_index_input(terms_enum_t *terms_enum) {
    if (terms_enum->in == NULL) {
        terms_enum->in = mram_reader_clone(terms_enum->field_reader->parent->terms_in);
    }
}

terms_enum_frame_t *push_frame_fp(terms_enum_t *terms_enum, arc_t *arc, uint64_t fp, uint32_t length) {
    terms_enum_frame_t *f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);
    f->arc = arc;

    if ((f->fp_orig == fp) && (f->next_ent != -1)) {
        if (f->ord > terms_enum->target_before_current_length) {
            rewind_frame(f);
        }
    } else {
        f->next_ent = -1;
        f->prefix = length;
        f->state->term_block_ord = 0;
        f->fp_orig = f->fp = fp;
        f->last_sub_fp = -1;
    }

    return f;
}

static terms_enum_frame_t *push_frame_bytes_ref(terms_enum_t *terms_enum, arc_t *arc, bytes_ref_t *frame_data, uint32_t length) {
    wram_reader_t reader = {
            .buffer = frame_data->bytes,
            .index = frame_data->offset
    };

    uint64_t code = wram_read_vlong(&reader);
    uint64_t fp_seek = code >> OUTPUT_FLAGS_NUM_BITS;
    terms_enum_frame_t *f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);
    f->has_terms = (code & OUTPUT_FLAG_HAS_TERMS) != 0;
    f->has_terms_orig = f->has_terms;
    f->is_floor = (code & OUTPUT_FLAG_IS_FLOOR) != 0;

    if (f->is_floor) {
        set_floor_data(f, &reader, frame_data);
    }

    push_frame_fp(terms_enum, arc, fp_seek, length);

    return f;
}

static terms_enum_frame_t *get_frame(terms_enum_t *terms_enum, int32_t ord) {
    if (ord >= terms_enum->stack_length) {
        terms_enum_frame_t *next = malloc((ord + 1) * sizeof(*next));
        memcpy(next, terms_enum->stack, ord * sizeof(*(terms_enum->stack)));
        for (int stack_ord = terms_enum->stack_length; stack_ord < ord + 1; ++stack_ord) {
            term_enum_frame_init(next + stack_ord, terms_enum, stack_ord);
        }

        terms_enum->stack = next;
        terms_enum->stack_length = (uint32_t) (ord + 1);
    }

    return terms_enum->stack + ord;
}

static arc_t *get_arc(terms_enum_t *terms_enum, int32_t ord) {
    if (ord >= terms_enum->arcs_length) {
        arc_t *next = malloc((ord + 1) * sizeof(*next));
        memcpy(next, terms_enum->arcs, ord * sizeof(*(terms_enum->arcs)));

        terms_enum->arcs = next;
        terms_enum->arcs_length = (uint32_t) (ord + 1);
    }

    return terms_enum->arcs + ord;
}

term_state_t *get_term_state(terms_enum_t *terms_enum) {
    term_state_t *result = malloc(sizeof(*result));
    decode_metadata(terms_enum->current_frame);
    memcpy(result, terms_enum->current_frame->state, sizeof(*result));
    return result;
}

int32_t get_doc_freq(terms_enum_t *terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state->doc_freq;
}

int64_t get_total_term_freq(terms_enum_t *terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state->total_term_freq;
}
