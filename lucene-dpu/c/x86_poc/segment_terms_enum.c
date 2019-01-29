/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "segment_terms_enum.h"
#include "math.h"
#include "allocation.h"

#define OUTPUT_FLAGS_NUM_BITS 2
#define OUTPUT_FLAG_IS_FLOOR 0x2
#define OUTPUT_FLAG_HAS_TERMS 0x2

#define BLOCK_SIZE 128

typedef enum {
    SEEK_STATUS_END,
    SEEK_STATUS_FOUND,
    SEEK_STATUS_NOT_FOUND
} seek_status_t;

static segment_terms_enum_frame_t* push_frame_bytes_ref(segment_terms_enum_t* terms_enum, arc_t* arc, bytes_ref_t* frame_data, uint32_t length);
static segment_terms_enum_frame_t* push_frame_fp(segment_terms_enum_t* terms_enum, arc_t* arc, uint64_t fp, uint32_t length);
static segment_terms_enum_frame_t* get_frame(segment_terms_enum_t* terms_enum, int32_t ord);
static arc_t* get_arc(segment_terms_enum_t* terms_enum, int32_t ord);

static void set_floor_data(segment_terms_enum_frame_t* frame, data_input_t* in, bytes_ref_t* source);
static void rewind_frame(segment_terms_enum_frame_t* frame);
static void scan_to_floor_frame(segment_terms_enum_frame_t* frame, bytes_ref_t* target);
static void load_next_floor_block(segment_terms_enum_frame_t* frame);
static void load_block(segment_terms_enum_frame_t* frame);
static seek_status_t scan_to_term(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only);
static seek_status_t scan_to_term_leaf(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only);
static seek_status_t scan_to_term_non_leaf(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only);
static void fill_term(segment_terms_enum_frame_t* frame);
static bool next_frame_entry(segment_terms_enum_frame_t* frame);
static void next_frame_leaf(segment_terms_enum_frame_t* frame);
static bool next_frame_non_leaf(segment_terms_enum_frame_t* frame);

static segment_terms_enum_frame_t* segment_term_enum_frame_new(segment_terms_enum_t* term_enum, int32_t ord);
static void segment_term_enum_frame_init(segment_terms_enum_frame_t* frame, segment_terms_enum_t* term_enum, int32_t ord);

static void init_index_input(segment_terms_enum_t* terms_enum);

static block_term_state_t* block_term_state_new(void);
static void decode_metadata(segment_terms_enum_frame_t* frame);
static void decode_term(int64_t* longs, data_input_t* in, field_info_t* field_info, block_term_state_t* state, bool absolute);

segment_terms_enum_t* segment_terms_enum_new(field_reader_t *field_reader) {
    segment_terms_enum_t* terms_enum = allocation_get(sizeof(*terms_enum));
    terms_enum->in = NULL;
    terms_enum->field_reader = field_reader;
    terms_enum->stack = NULL;
    terms_enum->stack_length = 0;
    terms_enum->static_frame = segment_term_enum_frame_new(terms_enum, -1);

    if (field_reader->index == NULL) {
        terms_enum->fst_reader = NULL;
    } else {
        terms_enum->fst_reader = fst_get_bytes_reader(field_reader->index);
    }

    terms_enum->arcs = allocation_get(sizeof(*(terms_enum->arcs)));
    memset(terms_enum->arcs, 0, sizeof(*(terms_enum->arcs)));
    terms_enum->arcs_length = 1;

    terms_enum->current_frame = terms_enum->static_frame;

    terms_enum->valid_index_prefix = 0;
    terms_enum->term = bytes_ref_new();

    return terms_enum;
}

block_term_state_t* get_term_state(segment_terms_enum_t* terms_enum) {
    decode_metadata(terms_enum->current_frame);
    block_term_state_t* result = allocation_get(sizeof(*result));
    memcpy(result, terms_enum->current_frame->state, sizeof(*result));
    return result;
}

int32_t get_doc_freq(segment_terms_enum_t* terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state->doc_freq;
}

int64_t get_total_term_freq(segment_terms_enum_t* terms_enum) {
    decode_metadata(terms_enum->current_frame);
    return terms_enum->current_frame->state->total_term_freq;
}

bool seek_exact(segment_terms_enum_t* terms_enum, bytes_ref_t* target) {
    arc_t* arc;
    bytes_ref_t* output;
    uint32_t target_up_to;

    bytes_ref_grow(terms_enum->term, 1 + target->length);
    terms_enum->target_before_current_length = terms_enum->current_frame->ord;

    if (terms_enum->current_frame != terms_enum->static_frame) {
        arc = &terms_enum->arcs[0];
        output = arc->output;
        target_up_to = 0;

        segment_terms_enum_frame_t* last_frame = &terms_enum->stack[0];
        uint32_t target_limit = min(target->length, terms_enum->valid_index_prefix);

        int32_t cmp = 0;

        while (target_up_to < target_limit) {
            cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) - (target->bytes[target->offset + target_up_to] & 0xFF);

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
                cmp = (terms_enum->term->bytes[target_up_to] & 0xFF) - (target->bytes[target->offset + target_up_to] & 0xFF);

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
        arc = get_first_arc(terms_enum->field_reader->index, &terms_enum->arcs[0]);
        output = arc->output;
        terms_enum->current_frame = terms_enum->static_frame;
        target_up_to = 0;
        terms_enum->current_frame = push_frame_bytes_ref(terms_enum, arc, bytes_ref_add(output, arc->next_final_output), 0);
    }

    while (target_up_to < target->length) {
        uint32_t target_label = (uint32_t) (target->bytes[target->offset + target_up_to] & 0xFF);

        arc_t* next_arc = find_target_arc(terms_enum->field_reader->index, target_label, arc, get_arc(terms_enum, 1 + target_up_to), terms_enum->fst_reader);

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
                terms_enum->current_frame = push_frame_bytes_ref(terms_enum, arc, bytes_ref_add(output, arc->next_final_output), target_up_to);
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

static segment_terms_enum_frame_t* segment_term_enum_frame_new(segment_terms_enum_t* term_enum, int32_t ord) {
    segment_terms_enum_frame_t* frame = allocation_get(sizeof(*frame));

    segment_term_enum_frame_init(frame, term_enum, ord);

    return frame;
}

static void segment_term_enum_frame_init(segment_terms_enum_frame_t* frame, segment_terms_enum_t* term_enum, int32_t ord) {
    frame->ste = term_enum;
    frame->ord = ord;
    frame->state = block_term_state_new();
    frame->state->total_term_freq = -1;
    frame->longs = allocation_get((term_enum->field_reader->longs_size) * sizeof(*(frame->longs)));
    frame->longs_length = term_enum->field_reader->longs_size;

    frame->suffix_bytes_length = 0;
    frame->stat_bytes_length = 0;
    frame->floor_data_length = 0;
    frame->bytes_length = 0;

    frame->suffixes_reader = allocation_get(sizeof(*(frame->suffixes_reader)));
    frame->suffixes_reader->read_byte = incremental_read_byte;
    frame->suffixes_reader->skip_bytes = incremental_skip_bytes;

    frame->floor_data_reader = allocation_get(sizeof(*(frame->floor_data_reader)));
    frame->floor_data_reader->read_byte = incremental_read_byte;
    frame->floor_data_reader->skip_bytes = incremental_skip_bytes;

    frame->stats_reader = allocation_get(sizeof(*(frame->stats_reader)));
    frame->stats_reader->read_byte = incremental_read_byte;
    frame->stats_reader->skip_bytes = incremental_skip_bytes;

    frame->bytes_reader = allocation_get(sizeof(*(frame->bytes_reader)));
    frame->bytes_reader->read_byte = incremental_read_byte;
    frame->bytes_reader->skip_bytes = incremental_skip_bytes;
}

static segment_terms_enum_frame_t* push_frame_bytes_ref(segment_terms_enum_t* terms_enum, arc_t* arc, bytes_ref_t* frame_data, uint32_t length) {
    data_input_t reader = {
            .buffer = frame_data->bytes,
            .index = frame_data->offset,
            .read_byte = incremental_read_byte,
            .skip_bytes = incremental_skip_bytes
    };

    uint64_t code = read_vlong(&reader);
    uint64_t fp_seek = code >> OUTPUT_FLAGS_NUM_BITS;
    segment_terms_enum_frame_t* f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);
    f->has_terms = (code & OUTPUT_FLAG_HAS_TERMS) != 0;
    f->has_terms_orig = f->has_terms;
    f->is_floor = (code & OUTPUT_FLAG_IS_FLOOR) != 0;

    if (f->is_floor) {
        set_floor_data(f, &reader, frame_data);
    }

    push_frame_fp(terms_enum, arc, fp_seek, length);

    return f;
}

static segment_terms_enum_frame_t* push_frame_fp(segment_terms_enum_t* terms_enum, arc_t* arc, uint64_t fp, uint32_t length) {
    segment_terms_enum_frame_t* f = get_frame(terms_enum, 1 + terms_enum->current_frame->ord);
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

static segment_terms_enum_frame_t* get_frame(segment_terms_enum_t* terms_enum, int32_t ord) {
    if (ord >= terms_enum->stack_length) {
        segment_terms_enum_frame_t* next = allocation_get((ord + 1) * sizeof(*next));
        memcpy(next, terms_enum->stack, ord * sizeof(*(terms_enum->stack)));
        for (int stack_ord = terms_enum->stack_length; stack_ord < ord + 1; ++stack_ord) {
            segment_term_enum_frame_init(next + stack_ord, terms_enum, stack_ord);
        }

        terms_enum->stack = next;
        terms_enum->stack_length = (uint32_t) (ord + 1);
    }

    return terms_enum->stack + ord;
}

static arc_t* get_arc(segment_terms_enum_t* terms_enum, int32_t ord) {
    if (ord >= terms_enum->arcs_length) {
        arc_t* next = allocation_get((ord + 1) * sizeof(*next));
        memcpy(next, terms_enum->arcs, ord * sizeof(*(terms_enum->arcs)));

        // todo new arcs init? find_target_arc should do it

        terms_enum->arcs = next;
        terms_enum->arcs_length = (uint32_t) (ord + 1);
    }

    return terms_enum->arcs + ord;
}

static void set_floor_data(segment_terms_enum_frame_t* frame, data_input_t* in, bytes_ref_t* source) {
    uint32_t num_bytes = source->length - (in->index - source->offset);
    if (num_bytes > frame->floor_data_length) {
        frame->floor_data = allocation_get(num_bytes);
        frame->floor_data_length = num_bytes;
    }
    memcpy(frame->floor_data, source->bytes + source->offset + in->index, num_bytes);
    frame->floor_data_reader->buffer = frame->floor_data;
    frame->floor_data_reader->index = 0;
    frame->num_follow_floor_blocks = read_vint(frame->floor_data_reader);
    frame->next_floor_label = (uint32_t) (frame->floor_data_reader->read_byte(frame->floor_data_reader) & 0xFF);
}

static void rewind_frame(segment_terms_enum_frame_t* frame) {
    frame->fp = frame->fp_orig;
    frame->next_ent = -1;
    frame->has_terms = frame->has_terms_orig;
    if (frame->is_floor) {
        frame->floor_data_reader->index = 0;
        frame->num_follow_floor_blocks = read_vint(frame->floor_data_reader);
        frame->next_floor_label = (uint32_t) (frame->floor_data_reader->read_byte(frame->floor_data_reader) & 0xFF);
    }
}

static void scan_to_floor_frame(segment_terms_enum_frame_t* frame, bytes_ref_t* target) {
    if (!frame->is_floor || (target->length < frame->prefix)) {
        return;
    }

    uint32_t target_label = (uint32_t) (target->bytes[target->offset + frame->prefix] & 0xFF);

    if (target_label < frame->next_floor_label) {
        return;
    }

    uint64_t new_fp;

    while (true) {
        uint64_t code = read_vlong(frame->floor_data_reader);
        new_fp = frame->fp_orig + (code >> 1);
        frame->has_terms = (code & 1) != 0;
        frame->is_last_in_floor = frame->num_follow_floor_blocks == 1;
        frame->num_follow_floor_blocks--;

        if (frame->is_last_in_floor) {
            frame->next_floor_label = 256;
            break;
        } else {
            frame->next_floor_label = (uint32_t) (frame->floor_data_reader->read_byte(frame->floor_data_reader) & 0xFF);
            if (target_label < frame->next_floor_label) {
                break;
            }
        }
    }

    if (new_fp != frame->fp) {
        frame->next_ent = -1;
        frame->fp = new_fp;
    }
}

static void load_next_floor_block(segment_terms_enum_frame_t* frame) {
    frame->fp = frame->fp_end;
    frame->next_ent = -1;
    load_block(frame);
}

static void load_block(segment_terms_enum_frame_t* frame) {
    init_index_input(frame->ste);

    if (frame->next_ent != -1) {
        return;
    }

    frame->ste->in->index = (uint32_t) frame->fp;
    uint32_t code = read_vint(frame->ste->in);
    frame->ent_count = code >> 1;
    frame->is_last_in_floor = (code & 1) != 0;
    code = read_vint(frame->ste->in);
    frame->is_leaf_block = (code & 1) != 0;
    uint32_t num_bytes = code >> 1;
    if (frame->suffix_bytes_length < num_bytes) {
        frame->suffix_bytes = allocation_get(num_bytes);
        frame->suffix_bytes_length = num_bytes;
    }
    read_bytes(frame->ste->in, frame->suffix_bytes, 0, num_bytes);
    frame->suffixes_reader->index = 0;
    frame->suffixes_reader->buffer = frame->suffix_bytes;

    num_bytes = read_vint(frame->ste->in);
    if (frame->stat_bytes_length < num_bytes) {
        frame->stat_bytes = allocation_get(num_bytes);
        frame->stat_bytes_length = num_bytes;
    }
    read_bytes(frame->ste->in, frame->stat_bytes, 0, num_bytes);
    frame->stats_reader->index = 0;
    frame->stats_reader->buffer = frame->stat_bytes;
    frame->metadata_up_to = 0;
    frame->state->term_block_ord = 0;
    frame->next_ent = 0;
    frame->last_sub_fp = -1;

    num_bytes = read_vint(frame->ste->in);
    if (frame->bytes_length < num_bytes) {
        frame->bytes = allocation_get(num_bytes);
        frame->bytes_length = num_bytes;
    }
    read_bytes(frame->ste->in, frame->bytes, 0, num_bytes);
    frame->bytes_reader->index = 0;
    frame->bytes_reader->buffer = frame->bytes;

    frame->fp_end = frame->ste->in->index;
}

static seek_status_t scan_to_term(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only) {
    return frame->is_leaf_block ? scan_to_term_leaf(frame, target, exact_only) : scan_to_term_non_leaf(frame, target, exact_only);
}

static seek_status_t scan_to_term_leaf(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only) {
    frame->ste->term_exists = true;
    frame->sub_code = 0;

    if (frame->next_ent == frame->ent_count) {
        if (exact_only) {
            fill_term(frame);
        }
        return SEEK_STATUS_END;
    }

    start_loop:
    while (true) {
        frame->next_ent++;
        frame->suffix = read_vint(frame->suffixes_reader);
        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader->index;
        frame->suffixes_reader->skip_bytes(frame->suffixes_reader, frame->suffix);
        uint32_t target_limit = target->offset + (target->length < term_len ? target->length : term_len);
        uint32_t target_pos = target->offset+ frame->prefix;
        uint32_t byte_pos = frame->start_byte_pos;
        while (true) {
            int32_t cmp;
            bool stop;
            if (target_pos < target_limit) {
                cmp = (frame->suffix_bytes[byte_pos++] & 0xFF) - (target->bytes[target_pos++] & 0xFF);
                stop = false;
            } else {
                cmp = term_len - target->length;
                stop = true;
            }

            if (cmp < 0) {
                if (frame->next_ent == frame->ent_count) {
                    goto end_loop;
                } else {
                    goto start_loop;
                }
            } else if (cmp > 0) {
                fill_term(frame);
                return SEEK_STATUS_NOT_FOUND;
            } else if (stop) {
                fill_term(frame);
                return SEEK_STATUS_FOUND;
            }
        }
    }
    end_loop:
    if (exact_only) {
        fill_term(frame);
    }

    return SEEK_STATUS_END;
}

static seek_status_t scan_to_term_non_leaf(segment_terms_enum_frame_t* frame, bytes_ref_t* target, bool exact_only) {
    if (frame->next_ent == frame->ent_count) {
        if (exact_only) {
            fill_term(frame);
            frame->ste->term_exists = frame->sub_code == 0;
        }
        return SEEK_STATUS_END;
    }

    start_loop:
    while (frame->next_ent < frame->ent_count) {
        frame->next_ent++;
        uint32_t code = read_vint(frame->suffixes_reader);
        frame->suffix = code >> 1;

        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader->index;
        frame->suffixes_reader->skip_bytes(frame->suffixes_reader, frame->suffix);
        frame->ste->term_exists = (code & 1) == 0;

        if (frame->ste->term_exists) {
            frame->state->term_block_ord++;
            frame->sub_code = 0;
        } else {
            frame->sub_code = read_vlong(frame->suffixes_reader);
            frame->last_sub_fp = frame->fp - frame->sub_code;
        }

        uint32_t target_limit = target->offset + (target->length < term_len ? target->length : term_len);
        uint32_t target_pos = target->offset + frame->prefix;

        uint32_t byte_pos = frame->start_byte_pos;
        while (true) {
            int32_t cmp;
            bool stop;

            if (target_pos < target_limit) {
                cmp = (frame->suffix_bytes[byte_pos++] & 0xFF) - (target->bytes[target_pos++] & 0xFF);
                stop = false;
            } else {
                cmp = term_len - target->length;
                stop = true;
            }

            if (cmp < 0) {
                goto start_loop;
            } else if (cmp > 0) {
                fill_term(frame);

                if (!exact_only && !frame->ste->term_exists) {
                    frame->ste->current_frame = push_frame_fp(frame->ste, NULL,
                                                              (uint64_t) frame->ste->current_frame->last_sub_fp, term_len);
                    load_block(frame->ste->current_frame);
                    while (next_frame_entry(frame->ste->current_frame)) {
                        frame->ste->current_frame = push_frame_fp(frame->ste, NULL,
                                                                  (uint64_t) frame->ste->current_frame->last_sub_fp, frame->ste->term->length);
                        load_block(frame->ste->current_frame);
                    }
                }

                return SEEK_STATUS_NOT_FOUND;
            } else if (stop) {
                fill_term(frame);

                return SEEK_STATUS_FOUND;
            }
        }
    }

    if (exact_only) {
        fill_term(frame);
    }

    return SEEK_STATUS_END;
}

static void fill_term(segment_terms_enum_frame_t* frame) {
    uint32_t term_len = frame->prefix + frame->suffix;
    frame->ste->term->length = term_len;
    bytes_ref_grow(frame->ste->term, term_len);
    memcpy(frame->ste->term->bytes + frame->prefix, frame->suffix_bytes + frame->start_byte_pos, frame->suffix);
}

static bool next_frame_entry(segment_terms_enum_frame_t* frame) {
    if (frame->is_leaf_block) {
        next_frame_leaf(frame);
        return false;
    } else {
        return next_frame_non_leaf(frame);
    }
}

static void next_frame_leaf(segment_terms_enum_frame_t* frame) {
    frame->next_ent++;
    frame->suffix = read_vint(frame->suffixes_reader);
    frame->start_byte_pos = frame->suffixes_reader->index;
    frame->ste->term->length = frame->prefix + frame->suffix;
    bytes_ref_grow(frame->ste->term, frame->ste->term->length);
    read_bytes(frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix);
    frame->ste->term_exists = true;
}

static bool next_frame_non_leaf(segment_terms_enum_frame_t* frame) {
    while (true) {
        if (frame->next_ent == frame->ent_count) {
            load_next_floor_block(frame);
            if (frame->is_leaf_block) {
                next_frame_leaf(frame);
                return false;
            } else {
                continue;
            }
        }

        frame->next_ent++;
        uint32_t code = read_vint(frame->suffixes_reader);
        frame->suffix = code >> 1;
        frame->start_byte_pos = frame->suffixes_reader->index;
        frame->ste->term->length = frame->prefix + frame->suffix;
        bytes_ref_grow(frame->ste->term, frame->ste->term->length);
        read_bytes(frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix);
        if ((code & 1) == 0) {
            frame->ste->term_exists = true;
            frame->sub_code = 0;
            frame->state->term_block_ord++;
            return false;
        } else {
            frame->ste->term_exists = false;
            frame->sub_code = read_vlong(frame->suffixes_reader);
            frame->last_sub_fp = frame->fp - frame->sub_code;
            return true;
        }
    }
}

static block_term_state_t* block_term_state_new(void) {
    block_term_state_t* result = allocation_get(sizeof(*result));

    result->last_pos_block_offset = -1;
    result->singleton_doc_id = -1;

    return result;
}

static void init_index_input(segment_terms_enum_t* terms_enum) {
    if (terms_enum->in == NULL) {
        terms_enum->in = data_input_clone(terms_enum->field_reader->parent->terms_in);
    }
}

static void decode_metadata(segment_terms_enum_frame_t* frame) {
    int32_t limit = frame->is_leaf_block ? frame->next_ent : frame->state->term_block_ord;
    bool absolute = frame->metadata_up_to == 0;

    while (frame->metadata_up_to < limit) {
        frame->state->doc_freq = read_vint(frame->stats_reader);
        if (frame->ste->field_reader->field_info->index_options == INDEX_OPTIONS_DOCS) {
            frame->state->total_term_freq = frame->state->doc_freq;
        } else {
            frame->state->total_term_freq = frame->state->doc_freq + read_vlong(frame->stats_reader);
        }
        for (int i = 0; i < frame->ste->field_reader->longs_size; ++i) {
            frame->longs[i] = read_vlong(frame->bytes_reader);
        }
        decode_term(frame->longs, frame->bytes_reader, frame->ste->field_reader->field_info, frame->state, absolute);
        frame->metadata_up_to++;
        absolute = false;
    }
    frame->state->term_block_ord = frame->metadata_up_to;
}

static void decode_term(int64_t* longs, data_input_t* in, field_info_t* field_info, block_term_state_t* state, bool absolute) {
    bool field_has_positions = (field_info->index_options - INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    bool field_has_offsets = (field_info->index_options - INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    bool field_has_payloads = field_info->store_payloads;

    if (absolute) {
        state->doc_start_fp = 0;
        state->pos_start_fp = 0;
        state->pay_start_fp = 0;
    }

    state->doc_start_fp += longs[0];
    if (field_has_positions) {
        state->pos_start_fp += longs[1];
        if (field_has_offsets || field_has_payloads) {
            state->pay_start_fp += longs[2];
        }
    }
    if (state->doc_freq == 1) {
        state->singleton_doc_id = read_vint(in);
    } else {
        state->singleton_doc_id = -1;
    }
    if (field_has_positions) {
        if (state->total_term_freq > BLOCK_SIZE) {
            state->last_pos_block_offset = read_vlong(in);
        } else {
            state->last_pos_block_offset = -1;
        }
    }
    if (state->doc_freq > BLOCK_SIZE) {
        state->skip_offset = read_vlong(in);
    } else {
        state->skip_offset = -1;
    }
}