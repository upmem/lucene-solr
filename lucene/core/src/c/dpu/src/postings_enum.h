/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __POSTINGS_ENUM_H__
#define __POSTINGS_ENUM_H__

#include <stdint.h>
#include <stdbool.h>

#include "mram_reader.h"
#include "terms_enum.h"
#include "for_util.h"

#define POSTINGS_ENUM_FREQS (1 << 3)
#define POSTINGS_ENUM_POSITIONS (1 << 4)

#define NO_MORE_DOCS 0x7fffffff

#define MAX_ENCODED_SIZE (BLOCK_SIZE * 4)

typedef struct {
    mram_reader_t *start_doc_in;
    mram_reader_t doc_in;
    bool doc_in_initialized;

    bool index_has_freq;
    bool index_has_pos;
    bool index_has_offsets;
    bool index_has_payloads;

    uint32_t doc_freq;
    uint64_t total_term_freq;
    uint64_t doc_term_start_fp;
    int64_t skip_offset;
    int32_t singleton_doc_id;

    int32_t doc;
    int32_t freq;
    bool needs_freq;
    int32_t *doc_delta_buffer;
    int32_t *freq_buffer;
    uint32_t accum;
    uint32_t doc_up_to;
    uint32_t next_skip_doc;
    uint32_t doc_buffer_up_to;
    bool skipped;

    uint8_t encoded[MAX_ENCODED_SIZE];

    flat_for_util_t *for_util;
} postings_enum_t;

void impacts(postings_enum_t *postings_enum, terms_enum_t *terms_enum, uint32_t flags, mram_reader_t *doc_reader, flat_for_util_t *for_util);

int32_t postings_next_doc(postings_enum_t *postings_enum);

#endif /* __POSTINGS_ENUM_H__ */
