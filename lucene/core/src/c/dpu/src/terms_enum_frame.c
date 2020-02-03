/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "terms_enum_frame.h"
#include "alloc_wrapper.h"
#include "field_info.h"
#include "terms_enum.h"
#include <defs.h>
#include <string.h>

#define MAX_LONGS_SIZE 3

static seek_status_t scan_to_term_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);
static seek_status_t scan_to_term_non_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);
static void fill_term(terms_enum_frame_t *frame);
static bool next_frame_entry(terms_enum_frame_t *frame);
static void next_frame_leaf(terms_enum_frame_t *frame);
static bool next_frame_non_leaf(terms_enum_frame_t *frame);

static void decode_term(int64_t *longs, mram_reader_t *in, flat_field_info_t *field_info, term_state_t *state, bool absolute)
{
    bool field_has_positions = compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    bool field_has_offsets
        = compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
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
        state->singleton_doc_id = mram_read_vint(in, false);
    } else {
        state->singleton_doc_id = -1;
    }
    if (field_has_positions) {
        if (state->total_term_freq > BLOCK_SIZE) {
            state->last_pos_block_offset = mram_read_vlong(in, false);
        } else {
            state->last_pos_block_offset = -1;
        }
    }
    if (state->doc_freq > BLOCK_SIZE) {
        state->skip_offset = mram_read_vlong(in, false);
    } else {
        state->skip_offset = -1;
    }
}

void decode_metadata(terms_enum_frame_t *frame)
{
    int32_t limit = frame->is_leaf_block ? frame->next_ent : frame->state.term_block_ord;
    bool absolute = frame->metadata_up_to == 0;

    while (frame->metadata_up_to < limit) {
        frame->state.doc_freq = mram_read_vint(&frame->stats_reader, false);
        if (frame->ste->field_reader->field_info.index_options == INDEX_OPTIONS_DOCS) {
            frame->state.total_term_freq = frame->state.doc_freq;
        } else {
            frame->state.total_term_freq = frame->state.doc_freq + mram_read_vlong(&frame->stats_reader, false);
        }
        if (frame->ste->field_reader->longs_size > MAX_LONGS_SIZE) {
            halt();
        }
        int64_t longs[MAX_LONGS_SIZE];
        for (int i = 0; i < frame->ste->field_reader->longs_size; ++i) {
            longs[i] = mram_read_vlong(&frame->bytes_reader, false);
        }
        decode_term(longs, &frame->bytes_reader, &frame->ste->field_reader->field_info, &frame->state, absolute);
        frame->metadata_up_to++;
        absolute = false;
    }
    frame->state.term_block_ord = frame->metadata_up_to;
}

void term_enum_frame_init(terms_enum_frame_t *frame, terms_enum_t *term_enum, int32_t ord)
{
    frame->ste = term_enum;
    frame->ord = ord;
    frame->state.last_pos_block_offset = -1;
    frame->state.singleton_doc_id = -1;
    frame->state.total_term_freq = -1;
}

void rewind_frame(terms_enum_frame_t *frame)
{
    frame->fp = frame->fp_orig;
    frame->next_ent = -1;
    frame->has_terms = frame->has_terms_orig;
    if (frame->is_floor) {
        frame->floor_data_reader.index = 0;
        frame->num_follow_floor_blocks = wram_read_vint(&frame->floor_data_reader);
        frame->next_floor_label = (uint32_t)(wram_read_byte(&frame->floor_data_reader) & 0xFF);
    }
}

void set_floor_data(terms_enum_frame_t *frame, wram_reader_t *in, bytes_ref_t *source)
{
    uint32_t num_bytes = source->length - (in->index - source->offset);
    if (num_bytes > MAX_FLOOR_DATA) {
        halt();
    }
    memcpy(frame->floor_data, source->bytes + source->offset + in->index, num_bytes);
    frame->floor_data_reader.buffer = frame->floor_data;
    frame->floor_data_reader.index = 0;
    frame->num_follow_floor_blocks = wram_read_vint(&frame->floor_data_reader);
    frame->next_floor_label = (uint32_t)(wram_read_byte(&frame->floor_data_reader) & 0xFF);
}

void scan_to_floor_frame(terms_enum_frame_t *frame, bytes_ref_t *target)
{
    if (!frame->is_floor || (target->length < frame->prefix)) {
        return;
    }

    uint32_t target_label = (uint32_t)(target->bytes[target->offset + frame->prefix] & 0xFF);

    if (target_label < frame->next_floor_label) {
        return;
    }

    uint32_t new_fp;

    while (true) {
        uint32_t code = wram_read_false_vlong(&frame->floor_data_reader);
        new_fp = frame->fp_orig + (code >> 1);
        frame->has_terms = (code & 1) != 0;
        frame->is_last_in_floor = frame->num_follow_floor_blocks == 1;
        frame->num_follow_floor_blocks--;

        if (frame->is_last_in_floor) {
            frame->next_floor_label = 256;
            break;
        } else {
            frame->next_floor_label = (uint32_t)(wram_read_byte(&frame->floor_data_reader) & 0xFF);
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

void load_next_floor_block(terms_enum_frame_t *frame)
{
    frame->fp = frame->fp_end;
    frame->next_ent = -1;
    load_block(frame);
}

void load_block(terms_enum_frame_t *frame)
{
    if (frame->next_ent != -1) {
        return;
    }

    set_index(&frame->ste->in, (uint32_t)frame->fp);
    uint32_t code = mram_read_vint(&frame->ste->in, false);
    frame->ent_count = code >> 1;
    frame->is_last_in_floor = (code & 1) != 0;
    code = mram_read_vint(&frame->ste->in, false);
    frame->is_leaf_block = (code & 1) != 0;
    uint32_t num_bytes = code >> 1;
    mram_reader_fill(&frame->suffixes_reader, &frame->ste->in);
    mram_skip_bytes(&frame->ste->in, num_bytes, false);

    num_bytes = mram_read_vint(&frame->ste->in, false);
    mram_reader_fill(&frame->stats_reader, &frame->ste->in);
    mram_skip_bytes(&frame->ste->in, num_bytes, false);

    frame->metadata_up_to = 0;
    frame->state.term_block_ord = 0;
    frame->next_ent = 0;
    frame->last_sub_fp = -1;

    num_bytes = mram_read_vint(&frame->ste->in, false);
    mram_reader_fill(&frame->bytes_reader, &frame->ste->in);
    mram_skip_bytes(&frame->ste->in, num_bytes, false);

    frame->fp_end = frame->ste->in.index - frame->ste->in.base;
}

seek_status_t scan_to_term(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only)
{
    return frame->is_leaf_block ? scan_to_term_leaf(frame, target, exact_only) : scan_to_term_non_leaf(frame, target, exact_only);
}

static seek_status_t scan_to_term_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only)
{
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
        frame->suffix = mram_read_vint(&frame->suffixes_reader, false);
        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader.index - frame->suffixes_reader.base;
        mram_skip_bytes(&frame->suffixes_reader, frame->suffix, false);
        uint32_t target_limit = target->offset + (target->length < term_len ? target->length : term_len);
        uint32_t target_pos = target->offset + frame->prefix;
        uint32_t byte_pos = frame->start_byte_pos;
        while (true) {
            int32_t cmp;
            bool stop;
            if (target_pos < target_limit) {
                uintptr_t previous_index = frame->suffixes_reader.index;
                set_index(&frame->suffixes_reader, byte_pos);
                uint8_t byte = mram_read_byte(&frame->suffixes_reader, false);
                frame->suffixes_reader.index = previous_index;
                byte_pos++;
                cmp = (byte & 0xFF) - (target->bytes[target_pos++] & 0xFF);
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

static seek_status_t scan_to_term_non_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only)
{
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
        uint32_t code = mram_read_vint(&frame->suffixes_reader, false);
        frame->suffix = code >> 1;

        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader.index - frame->suffixes_reader.base;
        mram_skip_bytes(&frame->suffixes_reader, frame->suffix, false);
        frame->ste->term_exists = (code & 1) == 0;

        if (frame->ste->term_exists) {
            frame->state.term_block_ord++;
            frame->sub_code = 0;
        } else {
            frame->sub_code = mram_read_false_vlong(&frame->suffixes_reader, false);
            frame->last_sub_fp = frame->fp - frame->sub_code;
        }

        uint32_t target_limit = target->offset + (target->length < term_len ? target->length : term_len);
        uint32_t target_pos = target->offset + frame->prefix;

        uint32_t byte_pos = frame->start_byte_pos;
        while (true) {
            int32_t cmp;
            bool stop;

            if (target_pos < target_limit) {
                uintptr_t previous_index = frame->suffixes_reader.index;
                set_index(&frame->suffixes_reader, byte_pos);
                uint8_t byte = mram_read_byte(&frame->suffixes_reader, false);
                frame->suffixes_reader.index = previous_index;
                byte_pos++;
                cmp = (byte & 0xFF) - (target->bytes[target_pos++] & 0xFF);
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
                    frame->ste->current_frame
                        = push_frame_fp(frame->ste, (uint32_t)frame->ste->current_frame->last_sub_fp, term_len);
                    load_block(frame->ste->current_frame);
                    while (next_frame_entry(frame->ste->current_frame)) {
                        frame->ste->current_frame = push_frame_fp(
                            frame->ste, (uint32_t)frame->ste->current_frame->last_sub_fp, frame->ste->term->length);
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

static void fill_term(terms_enum_frame_t *frame)
{
    uint32_t term_len = frame->prefix + frame->suffix;
    frame->ste->term->length = term_len;
    bytes_ref_grow(frame->ste->term, term_len);
    uintptr_t previous_index = frame->suffixes_reader.index;
    set_index(&frame->suffixes_reader, frame->start_byte_pos);
    mram_read_bytes(&frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix, false);
    frame->suffixes_reader.index = previous_index;
}

static bool next_frame_entry(terms_enum_frame_t *frame)
{
    if (frame->is_leaf_block) {
        next_frame_leaf(frame);
        return false;
    } else {
        return next_frame_non_leaf(frame);
    }
}

static void next_frame_leaf(terms_enum_frame_t *frame)
{
    frame->next_ent++;
    frame->suffix = mram_read_vint(&frame->suffixes_reader, false);
    frame->start_byte_pos = frame->suffixes_reader.index - frame->suffixes_reader.base;
    frame->ste->term->length = frame->prefix + frame->suffix;
    bytes_ref_grow(frame->ste->term, frame->ste->term->length);
    mram_read_bytes(&frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix, false);
    frame->ste->term_exists = true;
}

static bool next_frame_non_leaf(terms_enum_frame_t *frame)
{
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
        uint32_t code = mram_read_vint(&frame->suffixes_reader, false);
        frame->suffix = code >> 1;
        frame->start_byte_pos = frame->suffixes_reader.index - frame->suffixes_reader.base;
        frame->ste->term->length = frame->prefix + frame->suffix;
        bytes_ref_grow(frame->ste->term, frame->ste->term->length);
        mram_read_bytes(&frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix, false);
        if ((code & 1) == 0) {
            frame->ste->term_exists = true;
            frame->sub_code = 0;
            frame->state.term_block_ord++;
            return false;
        } else {
            frame->ste->term_exists = false;
            frame->sub_code = mram_read_false_vlong(&frame->suffixes_reader, false);
            frame->last_sub_fp = frame->fp - frame->sub_code;
            return true;
        }
    }
}
