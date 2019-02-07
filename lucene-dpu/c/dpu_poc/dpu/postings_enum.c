#include <stdlib.h>

#include "alloc_wrapper.h"
#include "postings_enum.h"
#include "terms_enum.h"

#define MAX_ENCODED_SIZE (BLOCK_SIZE * 4)

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
        postings_enum->doc_in->index = (uint32_t) postings_enum->doc_term_start_fp;
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

postings_enum_t *impacts(terms_enum_t *terms_enum, uint32_t flags, mram_reader_t *doc_reader, for_util_t *for_util) {
    decode_metadata(terms_enum->current_frame);

    field_info_t *field_info = terms_enum->field_reader->field_info;
    term_state_t *state = terms_enum->current_frame->state;

    bool index_has_positions =
            compare_index_options(field_info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;

    if (!index_has_positions || ((flags & POSTINGS_ENUM_POSITIONS) == 0)) {
        // todo no reuse

        postings_enum_t *postings_enum = malloc(sizeof(*postings_enum));

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
        postings_enum->encoded = malloc(MAX_ENCODED_SIZE);

        postings_enum->for_util = for_util;

        initialize_max_data_size(for_util);

        postings_enum->freq_buffer = malloc(MAX_DATA_SIZE * sizeof(*(postings_enum->freq_buffer)));
        postings_enum->doc_delta_buffer = malloc(MAX_DATA_SIZE * sizeof(*(postings_enum->doc_delta_buffer)));

        return postings_enum_reset(postings_enum, state, flags);
    } else {
        abort();
    }
}

