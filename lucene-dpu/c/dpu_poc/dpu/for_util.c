/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include "math.h"

#include "terms_enum.h"
#include "for_util.h"
#include "alloc_wrapper.h"

static uint32_t encoded_size(uint32_t bits_per_value) {
    return (uint32_t) (BLOCK_SIZE * bits_per_value + 7) / 8;
}

static uint32_t compute_iterations(packed_int_decoder_t *decoder) {
    return (uint32_t) (BLOCK_SIZE * decoder->byte_value_count);
}

static void fill_decoder(packed_int_decoder_t *decoder, uint32_t bits_per_value) {
    decoder->bits_per_value = bits_per_value;
    uint32_t blocks = bits_per_value;
    while ((blocks & 1) == 0) {
        blocks >>= 1;
    }
    decoder->long_block_count = blocks;
    decoder->long_value_count = 64 * decoder->long_block_count / bits_per_value;
    uint32_t byte_block_count = 8 * decoder->long_block_count;
    uint32_t byte_value_count = decoder->long_value_count;
    while ((byte_block_count & 1) == 0 && (byte_value_count & 1) == 0) {
        byte_block_count >>= 1;
        byte_value_count >>= 1;
    }
    decoder->byte_block_count = byte_block_count;
    decoder->byte_value_count = byte_value_count;
    if (bits_per_value == 64) {
        decoder->mask = ~0L;
    } else {
        decoder->mask = (1L << bits_per_value) - 1;
    }
    decoder->int_mask = (uint32_t) decoder->mask;
}

for_util_t *build_for_util(mram_reader_t *doc_reader) {
    for_util_t *for_util = malloc(sizeof(*for_util));

    uint32_t packed_ints_version = mram_read_vint(doc_reader, false);

    for_util->setup_done = malloc(33 * sizeof(*(for_util->setup_done)));
    for_util->encoded_sizes = malloc(33 * sizeof(*(for_util->encoded_sizes)));
    for_util->decoders = malloc(33 * sizeof(*(for_util->decoders)));
    for_util->iterations = malloc(33 * sizeof(*(for_util->iterations)));

    for (int i = 1; i < 33; ++i) {
        uint32_t code = mram_read_vint(doc_reader, false);
        uint32_t format_id = code >> 5;
        uint32_t bits_per_value = (code & 31) + 1;

        switch (format_id) {
            case 0:
                // PACKED: ok
                break;
            default:
                abort();
        }

        for_util->encoded_sizes[i] = encoded_size(bits_per_value);
        fill_decoder(for_util->decoders + i, bits_per_value);
        for_util->iterations[i] = compute_iterations(&for_util->decoders[i]);
        for_util->setup_done[i] = true;
    }

    return for_util;
}
