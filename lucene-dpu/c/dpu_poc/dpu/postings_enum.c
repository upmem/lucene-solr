#include <stdlib.h>

#include "alloc_wrapper.h"
#include "postings_enum.h"
#include "terms_enum.h"
#include "terms_enum_frame.h"

#define ALL_VALUES_EQUAL 0

static uint32_t MAX_DATA_SIZE = 0xFFFFFFFF;
#define math_max(x, y) (((x) > (y)) ? (x) : (y))

static void initialize_max_data_size(for_util_t *for_util) {
    if (MAX_DATA_SIZE == 0xFFFFFFFF) {
        uint32_t max_data_size = 0;
        for (int i = 1; i <= 32; ++i) {
            if (!for_util->setup_done[i]) {
                // todo simplified (one version, one packed_ints format)
                continue;
            }
            packed_int_decoder_t *decoder = for_util->decoders + i;
            uint32_t iterations = (uint32_t) ((BLOCK_SIZE + decoder->byte_value_count - 1) / decoder->byte_value_count);
            max_data_size = math_max(max_data_size, iterations * decoder->byte_value_count);
        }

        MAX_DATA_SIZE = max_data_size;
    }
}

static postings_enum_t *postings_enum_reset(postings_enum_t *postings_enum, term_state_t *state, uint32_t flags) {
    postings_enum->doc_freq = state->doc_freq;
    postings_enum->total_term_freq = (uint64_t) (postings_enum->index_has_freq ? state->total_term_freq
                                                                               : postings_enum->doc_freq);
    postings_enum->doc_term_start_fp = state->doc_start_fp;
    postings_enum->skip_offset = state->skip_offset;
    postings_enum->singleton_doc_id = state->singleton_doc_id;
    if (postings_enum->doc_freq > 1) {
        if (postings_enum->doc_in == NULL) {
            postings_enum->doc_in = mram_reader_clone(postings_enum->start_doc_in);
        }
        set_index(postings_enum->doc_in, (uint32_t) postings_enum->doc_term_start_fp);
    }

    postings_enum->doc = -1;
    postings_enum->needs_freq = (flags & POSTINGS_ENUM_FREQS) != 0;
    if (!postings_enum->index_has_freq || !postings_enum->needs_freq) {
        for (int i = 0; i < MAX_DATA_SIZE; ++i) {
            postings_enum->freq_buffer[i] = 1;
        }
    }
    postings_enum->accum = 0;
    postings_enum->doc_up_to = 0;
    postings_enum->next_skip_doc = BLOCK_SIZE - 1;
    postings_enum->doc_buffer_up_to = BLOCK_SIZE;
    postings_enum->skipped = false;

    return postings_enum;
}

void impacts(postings_enum_t *postings_enum, terms_enum_t *terms_enum, uint32_t flags, mram_reader_t *doc_reader, for_util_t *for_util) {
    decode_metadata(terms_enum->current_frame);

    field_info_t *field_info = terms_enum->field_reader->field_info;
    term_state_t *state = &terms_enum->current_frame->state;

    bool index_has_positions =
            compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;

    if (!index_has_positions || ((flags & POSTINGS_ENUM_POSITIONS) == 0)) {
        // todo no reuse

        postings_enum->start_doc_in = doc_reader;
        postings_enum->doc_in = NULL;

        postings_enum->index_has_freq =
                compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS) >= 0;
        postings_enum->index_has_pos =
                compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        postings_enum->index_has_offsets = compare_index_options(field_info->index_options,
                                                                 INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >=
                                           0;
        postings_enum->index_has_payloads = field_info->store_payloads;

        postings_enum->for_util = for_util;

        initialize_max_data_size(for_util);

        postings_enum->freq_buffer = malloc(MAX_DATA_SIZE * sizeof(*(postings_enum->freq_buffer)));
        postings_enum->doc_delta_buffer = malloc(MAX_DATA_SIZE * sizeof(*(postings_enum->doc_delta_buffer)));

        postings_enum_reset(postings_enum, state, flags);
    } else {
        abort();
    }
}

static void decode(packed_int_decoder_t *decoder,
                   uint8_t *blocks,
                   uint32_t blocks_offset,
                   int32_t *values,
                   uint32_t values_offset,
                   uint32_t iterations) {
    int32_t next_value = 0;
    uint32_t bits_left = decoder->bits_per_value;
    for (int i = 0; i < iterations * decoder->byte_block_count; ++i) {
        uint32_t bytes = (uint32_t) (blocks[blocks_offset++] & 0xFF);
        if (bits_left > 8) {
            // just buffer
            bits_left -= 8;
            next_value |= bytes << bits_left;
        } else {
            // flush
            int bits = 8 - bits_left;
            values[values_offset++] = next_value | (bytes >> bits);
            while (bits >= decoder->bits_per_value) {
                bits -= decoder->bits_per_value;
                values[values_offset++] = (bytes >> bits) & decoder->int_mask;
            }
            // then buffer
            bits_left = decoder->bits_per_value - bits;
            next_value = (bytes & ((1 << bits) - 1)) << bits_left;
        }
    }
}

static void read_block(mram_reader_t *in, for_util_t *for_util, uint8_t *encoded, int32_t *decoded) {
    uint32_t num_bits = mram_read_byte(in, false);

    if (num_bits == ALL_VALUES_EQUAL) {
        int32_t value = mram_read_vint(in, false);
        for (int i = 0; i < BLOCK_SIZE; ++i) {
            decoded[i] = value;
        }
        return;
    }

    if (!for_util->setup_done[num_bits]) {
        abort();
    }

    uint32_t encoded_size = for_util->encoded_sizes[num_bits];
    mram_read_bytes(in, encoded, 0, encoded_size, false);

    packed_int_decoder_t *decoder = for_util->decoders + num_bits;
    uint32_t iters = for_util->iterations[num_bits];

    decode(decoder, encoded, 0, decoded, 0, iters);
}

static void skip_block(mram_reader_t *in, for_util_t *for_util) {
    uint32_t num_bits = mram_read_byte(in, false);

    if (num_bits == ALL_VALUES_EQUAL) {
        mram_read_vint(in, false);
        return;
    }

    if (!for_util->setup_done[num_bits]) {
        abort();
    }

    uint32_t encoded_size = for_util->encoded_sizes[num_bits];
    in->index += encoded_size;
}

static void read_vint_block(mram_reader_t *doc_in,
                            int32_t *doc_buffer,
                            int32_t *freq_buffer,
                            uint32_t num,
                            bool index_has_freq) {
    if (index_has_freq) {
        for (int i = 0; i < num; i++) {
            uint32_t code = mram_read_vint(doc_in, false);
            doc_buffer[i] = code >> 1;
            if ((code & 1) != 0) {
                freq_buffer[i] = 1;
            } else {
                freq_buffer[i] = mram_read_vint(doc_in, false);
            }
        }
    } else {
        for (int i = 0; i < num; i++) {
            doc_buffer[i] = mram_read_vint(doc_in, false);
        }
    }
}

static void postings_refill_docs(postings_enum_t *postings_enum) {
    uint32_t left = postings_enum->doc_freq - postings_enum->doc_up_to;

    if (left >= BLOCK_SIZE) {
        read_block(postings_enum->doc_in, postings_enum->for_util, postings_enum->encoded,
                   postings_enum->doc_delta_buffer);

        if (postings_enum->index_has_freq) {
            if (postings_enum->needs_freq) {
                read_block(postings_enum->doc_in, postings_enum->for_util, postings_enum->encoded,
                           postings_enum->freq_buffer);
            } else {
                skip_block(postings_enum->doc_in, postings_enum->for_util);
            }
        }
    } else if (postings_enum->doc_freq == 1) {
        postings_enum->doc_delta_buffer[0] = postings_enum->singleton_doc_id;
        postings_enum->freq_buffer[0] = (int32_t) postings_enum->total_term_freq;
    } else {
        read_vint_block(postings_enum->doc_in, postings_enum->doc_delta_buffer, postings_enum->freq_buffer, left,
                        postings_enum->index_has_freq);
    }

    postings_enum->doc_buffer_up_to = 0;
}

int32_t postings_next_doc(postings_enum_t *postings_enum) {
    if (postings_enum->doc_up_to == postings_enum->doc_freq) {
        return postings_enum->doc = NO_MORE_DOCS;
    }
    if (postings_enum->doc_buffer_up_to == BLOCK_SIZE) {
        postings_refill_docs(postings_enum);
    }

    postings_enum->accum += postings_enum->doc_delta_buffer[postings_enum->doc_buffer_up_to];
    postings_enum->doc_up_to++;
    postings_enum->doc = postings_enum->accum;
    postings_enum->freq = postings_enum->freq_buffer[postings_enum->doc_buffer_up_to];
    postings_enum->doc_buffer_up_to++;

    return postings_enum->doc;
}


