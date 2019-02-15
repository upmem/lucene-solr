/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>
#include <string.h>

#include "mram_cache.h"
#include <defs.h>
#include "for_util.h"

#include "norms.h"
#include "context.h"
#include "mram_structure.h"
#include "dpu_characteristics.h"
#include "alloc_wrapper.h"
#include "terms_enum_frame.h"
#include <search_context.h>

static terms_enum_t terms_enums[NR_THREADS];
static bytes_ref_t empty_outputs_bytes_ref[NR_THREADS];

static __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) query_t the_query;
static __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) flat_search_context_t flat_contexts[NR_THREADS];
static __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) flat_field_reader_t field_readers[NR_THREADS];

__attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) uint64_t flat_context_offsets[NR_THREADS];

static inline uint32_t dma_aligned(uint32_t x) {
    return (x + (MRAM_ACCESS_ALIGNMENT - 1)) & ~(MRAM_ACCESS_ALIGNMENT - 1);
}

query_t *fetch_query(bool do_init) {
    if (do_init) {
        read_query(&the_query);
    }

    return &the_query;
}

terms_enum_t *initialize_terms_enum(uint32_t index, flat_field_reader_t *field_reader, mram_reader_t *terms_in) {
    terms_enum_t *terms_enum = terms_enums + index;

    terms_enum->field_reader = field_reader;
    mram_reader_fill(&terms_enum->in, terms_in);
    terms_enum->stack_length = 0;
    term_enum_frame_init(&terms_enum->static_frame, terms_enum, -1);

    memset(terms_enum->arcs, 0, sizeof(*(terms_enum->arcs)));
    terms_enum->arcs_length = 1;

    terms_enum->current_frame = &terms_enum->static_frame;

    terms_enum->valid_index_prefix = 0;
    terms_enum->term = bytes_ref_new();

    terms_enum->fst_reader.base = field_reader->index.mram_start_offset;
    terms_enum->fst_reader.index = field_reader->index.mram_start_offset + field_reader->index.mram_length - 1;
    terms_enum->fst_reader.cache = mram_cache_for(me());

    return terms_enum;
}

flat_search_context_t* initialize_flat_context(uint32_t index) {
    flat_search_context_t* context = flat_contexts + index;

    MRAM_READ(&flat_context_offsets[index], SEGMENT_SUMMARY_OFFSET + index * SEGMENT_SUMMARY_ENTRY_SIZE, SEGMENT_SUMMARY_ENTRY_SIZE);
    mram_readX((mram_addr_t) flat_context_offsets[index], context, sizeof(*context));

    uint32_t variable_length_content_size = dma_aligned(context->term_reader.nr_fields * sizeof(flat_field_reader_t))
        + dma_aligned(context->nr_norms_entries * sizeof(flat_norms_entry_t));
    uint32_t empty_outputs_size = dma_aligned(context->empty_outputs_length);
    context->empty_outputs = malloc(empty_outputs_size);
    mram_readX((mram_addr_t) (flat_context_offsets[index] + dma_aligned(sizeof(*context)) + variable_length_content_size),
               context->empty_outputs,
               empty_outputs_size);

    mram_cache_t* cache = mram_cache_for(index);
    cache->cached = 0xFFFFFFFF;

    context->term_reader.terms_in.cache = cache;
    context->norms_data.cache = cache;
    context->doc_reader.cache = cache;

    return context;
}

flat_field_reader_t *fetch_flat_field_reader(flat_search_context_t *context, const uint32_t field_id) {
    unsigned int task_id = me();
    flat_field_reader_t *res = &field_readers[task_id];
    mram_readX(flat_context_offsets[task_id] + dma_aligned(sizeof(*context))
               + dma_aligned(context->nr_norms_entries * sizeof(flat_norms_entry_t))
               + field_id * sizeof(flat_field_reader_t),
               res,
               sizeof(flat_field_reader_t));

    uint8_t *empty_outputs = context->empty_outputs;
    flat_fst_t* fst = &res->index;
    if (fst->empty_output_offset >= 0) {
        uint8_t num_bytes = (empty_outputs[fst->empty_output_offset] & 0xff) |
            ((empty_outputs[fst->empty_output_offset + 1] & 0xff) << 8)  |
            ((empty_outputs[fst->empty_output_offset + 2] & 0xff) << 16) |
            ((empty_outputs[fst->empty_output_offset + 3] & 0xff) << 24) ;

        if (num_bytes != 0) {
            fst->empty_output = &empty_outputs_bytes_ref[task_id];
            fst->empty_output->bytes = empty_outputs;
            fst->empty_output->offset = fst->empty_output_offset + 4;
            fst->empty_output->length = num_bytes;
            fst->empty_output->capacity = num_bytes;
        } else {
            fst->empty_output = NULL;
        }
    }

    return res;
}
