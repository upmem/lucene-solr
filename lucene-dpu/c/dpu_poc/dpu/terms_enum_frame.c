/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include "terms_enum_frame.h"
#include "alloc_wrapper.h"

static seek_status_t scan_to_term_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);
static seek_status_t scan_to_term_non_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only);
static void fill_term(terms_enum_frame_t *frame);
static bool next_frame_entry(terms_enum_frame_t *frame);
static void next_frame_leaf(terms_enum_frame_t *frame);
static bool next_frame_non_leaf(terms_enum_frame_t *frame);

static void decode_term(int64_t *longs,
                        wram_reader_t *in,
                        field_info_t *field_info,
                        term_state_t *state,
                        bool absolute) {
    bool field_has_positions =
            compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    bool field_has_offsets =
            compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
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
        state->singleton_doc_id = wram_read_vint(in);
    } else {
        state->singleton_doc_id = -1;
    }
    if (field_has_positions) {
        if (state->total_term_freq > BLOCK_SIZE) {
            state->last_pos_block_offset = wram_read_vlong(in);
        } else {
            state->last_pos_block_offset = -1;
        }
    }
    if (state->doc_freq > BLOCK_SIZE) {
        state->skip_offset = wram_read_vlong(in);
    } else {
        state->skip_offset = -1;
    }
}

void decode_metadata(terms_enum_frame_t *frame) {
    int32_t limit = frame->is_leaf_block ? frame->next_ent : frame->state->term_block_ord;
    bool absolute = frame->metadata_up_to == 0;

    while (frame->metadata_up_to < limit) {
        frame->state->doc_freq = wram_read_vint(frame->stats_reader);
        if (frame->ste->field_reader->field_info->index_options == INDEX_OPTIONS_DOCS) {
            frame->state->total_term_freq = frame->state->doc_freq;
        } else {
            frame->state->total_term_freq = frame->state->doc_freq + wram_read_vlong(frame->stats_reader);
        }
        for (int i = 0; i < frame->ste->field_reader->longs_size; ++i) {
            frame->longs[i] = wram_read_vlong(frame->bytes_reader);
        }
        decode_term(frame->longs, frame->bytes_reader, frame->ste->field_reader->field_info, frame->state, absolute);
        frame->metadata_up_to++;
        absolute = false;
    }
    frame->state->term_block_ord = frame->metadata_up_to;
}

terms_enum_frame_t *term_enum_frame_new(terms_enum_t *term_enum, int32_t ord) {
    terms_enum_frame_t *frame = malloc(sizeof(*frame));

    term_enum_frame_init(frame, term_enum, ord);

    return frame;
}

void term_enum_frame_init(terms_enum_frame_t *frame, terms_enum_t *term_enum, int32_t ord) {
    frame->ste = term_enum;
    frame->ord = ord;
    frame->state = block_term_state_new();
    frame->state->total_term_freq = -1;
    frame->longs = malloc((term_enum->field_reader->longs_size) * sizeof(*(frame->longs)));
    frame->longs_length = term_enum->field_reader->longs_size;

    frame->suffix_bytes_length = 0;
    frame->stat_bytes_length = 0;
    frame->floor_data_length = 0;
    frame->bytes_length = 0;

    frame->suffixes_reader = malloc(sizeof(*(frame->suffixes_reader)));
    frame->floor_data_reader = malloc(sizeof(*(frame->floor_data_reader)));
    frame->stats_reader = malloc(sizeof(*(frame->stats_reader)));
    frame->bytes_reader = malloc(sizeof(*(frame->bytes_reader)));
}

void rewind_frame(terms_enum_frame_t *frame) {
    frame->fp = frame->fp_orig;
    frame->next_ent = -1;
    frame->has_terms = frame->has_terms_orig;
    if (frame->is_floor) {
        frame->floor_data_reader->index = 0;
        frame->num_follow_floor_blocks = wram_read_vint(frame->floor_data_reader);
        frame->next_floor_label = (uint32_t) (wram_read_byte(frame->floor_data_reader) & 0xFF);
    }
}

void set_floor_data(terms_enum_frame_t *frame, wram_reader_t *in, bytes_ref_t *source) {
    uint32_t num_bytes = source->length - (in->index - source->offset);
    if (num_bytes > frame->floor_data_length) {
        frame->floor_data = malloc(num_bytes);
        frame->floor_data_length = num_bytes;
    }
    memcpy(frame->floor_data, source->bytes + source->offset + in->index, num_bytes);
    frame->floor_data_reader->buffer = frame->floor_data;
    frame->floor_data_reader->index = 0;
    frame->num_follow_floor_blocks = wram_read_vint(frame->floor_data_reader);
    frame->next_floor_label = (uint32_t) (wram_read_byte(frame->floor_data_reader) & 0xFF);
}

void scan_to_floor_frame(terms_enum_frame_t *frame, bytes_ref_t *target) {
    if (!frame->is_floor || (target->length < frame->prefix)) {
        return;
    }

    uint32_t target_label = (uint32_t) (target->bytes[target->offset + frame->prefix] & 0xFF);

    if (target_label < frame->next_floor_label) {
        return;
    }

    uint64_t new_fp;

    while (true) {
        uint64_t code = wram_read_vlong(frame->floor_data_reader);
        new_fp = frame->fp_orig + (code >> 1);
        frame->has_terms = (code & 1) != 0;
        frame->is_last_in_floor = frame->num_follow_floor_blocks == 1;
        frame->num_follow_floor_blocks--;

        if (frame->is_last_in_floor) {
            frame->next_floor_label = 256;
            break;
        } else {
            frame->next_floor_label = (uint32_t) (wram_read_byte(frame->floor_data_reader) & 0xFF);
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

void load_next_floor_block(terms_enum_frame_t *frame) {
    frame->fp = frame->fp_end;
    frame->next_ent = -1;
    load_block(frame);
}

void load_block(terms_enum_frame_t *frame) {
    init_index_input(frame->ste);

    if (frame->next_ent != -1) {
        return;
    }

    set_index(frame->ste->in, (uint32_t) frame->fp);
    uint32_t code = mram_read_vint(frame->ste->in, false);
    frame->ent_count = code >> 1;
    frame->is_last_in_floor = (code & 1) != 0;
    code = mram_read_vint(frame->ste->in, false);
    frame->is_leaf_block = (code & 1) != 0;
    uint32_t num_bytes = code >> 1;
    if (frame->suffix_bytes_length < num_bytes) {
        frame->suffix_bytes = malloc(num_bytes);
        frame->suffix_bytes_length = num_bytes;
    }
    mram_read_bytes(frame->ste->in, frame->suffix_bytes, 0, num_bytes, false);
    frame->suffixes_reader->index = 0;
    frame->suffixes_reader->buffer = frame->suffix_bytes;

    num_bytes = mram_read_vint(frame->ste->in, false);
    if (frame->stat_bytes_length < num_bytes) {
        frame->stat_bytes = malloc(num_bytes);
        frame->stat_bytes_length = num_bytes;
    }
    mram_read_bytes(frame->ste->in, frame->stat_bytes, 0, num_bytes, false);
    frame->stats_reader->index = 0;
    frame->stats_reader->buffer = frame->stat_bytes;
    frame->metadata_up_to = 0;
    frame->state->term_block_ord = 0;
    frame->next_ent = 0;
    frame->last_sub_fp = -1;

    num_bytes = mram_read_vint(frame->ste->in, false);
    if (frame->bytes_length < num_bytes) {
        frame->bytes = malloc(num_bytes);
        frame->bytes_length = num_bytes;
    }
    mram_read_bytes(frame->ste->in, frame->bytes, 0, num_bytes, false);
    frame->bytes_reader->index = 0;
    frame->bytes_reader->buffer = frame->bytes;

    frame->fp_end = frame->ste->in->index;
}

seek_status_t scan_to_term(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only) {
    return frame->is_leaf_block ? scan_to_term_leaf(frame, target, exact_only) : scan_to_term_non_leaf(frame, target, exact_only);
}

static seek_status_t scan_to_term_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only) {
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
        frame->suffix = wram_read_vint(frame->suffixes_reader);
        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader->index;
        wram_skip_bytes(frame->suffixes_reader, frame->suffix);
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

static seek_status_t scan_to_term_non_leaf(terms_enum_frame_t *frame, bytes_ref_t *target, bool exact_only) {
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
        uint32_t code = wram_read_vint(frame->suffixes_reader);
        frame->suffix = code >> 1;

        uint32_t term_len = frame->prefix + frame->suffix;
        frame->start_byte_pos = frame->suffixes_reader->index;
        wram_skip_bytes(frame->suffixes_reader, frame->suffix);
        frame->ste->term_exists = (code & 1) == 0;

        if (frame->ste->term_exists) {
            frame->state->term_block_ord++;
            frame->sub_code = 0;
        } else {
            frame->sub_code = wram_read_vlong(frame->suffixes_reader);
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
                                                              (uint64_t) frame->ste->current_frame->last_sub_fp,
                                                              term_len);
                    load_block(frame->ste->current_frame);
                    while (next_frame_entry(frame->ste->current_frame)) {
                        frame->ste->current_frame = push_frame_fp(frame->ste, NULL,
                                                                  (uint64_t) frame->ste->current_frame->last_sub_fp,
                                                                  frame->ste->term->length);
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

static void fill_term(terms_enum_frame_t *frame) {
    uint32_t term_len = frame->prefix + frame->suffix;
    frame->ste->term->length = term_len;
    bytes_ref_grow(frame->ste->term, term_len);
    memcpy(frame->ste->term->bytes + frame->prefix, frame->suffix_bytes + frame->start_byte_pos, frame->suffix);
}

static bool next_frame_entry(terms_enum_frame_t *frame) {
    if (frame->is_leaf_block) {
        next_frame_leaf(frame);
        return false;
    } else {
        return next_frame_non_leaf(frame);
    }
}

static void next_frame_leaf(terms_enum_frame_t *frame) {
    frame->next_ent++;
    frame->suffix = wram_read_vint(frame->suffixes_reader);
    frame->start_byte_pos = frame->suffixes_reader->index;
    frame->ste->term->length = frame->prefix + frame->suffix;
    bytes_ref_grow(frame->ste->term, frame->ste->term->length);
    wram_read_bytes(frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix);
    frame->ste->term_exists = true;
}

static bool next_frame_non_leaf(terms_enum_frame_t *frame) {
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
        uint32_t code = wram_read_vint(frame->suffixes_reader);
        frame->suffix = code >> 1;
        frame->start_byte_pos = frame->suffixes_reader->index;
        frame->ste->term->length = frame->prefix + frame->suffix;
        bytes_ref_grow(frame->ste->term, frame->ste->term->length);
        wram_read_bytes(frame->suffixes_reader, frame->ste->term->bytes, frame->prefix, frame->suffix);
        if ((code & 1) == 0) {
            frame->ste->term_exists = true;
            frame->sub_code = 0;
            frame->state->term_block_ord++;
            return false;
        } else {
            frame->ste->term_exists = false;
            frame->sub_code = wram_read_vlong(frame->suffixes_reader);
            frame->last_sub_fp = frame->fp - frame->sub_code;
            return true;
        }
    }
}
