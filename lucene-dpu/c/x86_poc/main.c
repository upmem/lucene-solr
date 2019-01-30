/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "parser.h"
#include "segment_terms_enum.h"
#include "term.h"
#include "allocation.h"
#include "norms.h"
#include "term_scorer.h"

typedef struct {
    file_buffer_t *file_buffers;
    block_tree_term_reader_t *term_reader;
    norms_reader_t *norms_reader;
    field_infos_t *field_infos;

    data_input_t *doc_in;
    for_util_t *for_util;
} lucene_global_context_t;

static block_tree_term_reader_t *build_fields_producer(lucene_global_context_t *ctx);

static data_input_t *build_doc_in(lucene_global_context_t *ctx);

static void search(lucene_global_context_t *ctx, char *field, char *value);

static norms_reader_t *build_norms_producer(lucene_global_context_t *ctx) {
    field_infos_t *field_infos = ctx->field_infos;

    file_buffer_t *buffer_norms_data = ctx->file_buffers + LUCENE_FILE_NVD;
    file_buffer_t *buffer_norms_metadata = ctx->file_buffers + LUCENE_FILE_NVM;

    data_input_t *norms_data = allocation_get(sizeof(*norms_data));
    norms_data->index = 0;
    norms_data->buffer = buffer_norms_data->content;
    norms_data->read_byte = incremental_read_byte;
    norms_data->skip_bytes = incremental_skip_bytes;

    data_input_t *norms_metadata = allocation_get(sizeof(*norms_metadata));
    norms_metadata->index = 0;
    norms_metadata->buffer = buffer_norms_metadata->content;
    norms_metadata->read_byte = incremental_read_byte;
    norms_metadata->skip_bytes = incremental_skip_bytes;

    return norms_reader_new(norms_metadata, buffer_norms_metadata->length,
                            norms_data, buffer_norms_data->length,
                            field_infos);
}

int main(int argc, char **argv) {
    lucene_global_context_t ctx;
    ctx.file_buffers = get_file_buffers(argv[1], 0);
    ctx.field_infos = read_field_infos(ctx.file_buffers + LUCENE_FILE_FNM);
    ctx.term_reader = build_fields_producer(&ctx);
    ctx.norms_reader = build_norms_producer(&ctx);
    ctx.doc_in = build_doc_in(&ctx);
    ctx.for_util = build_for_util(ctx.doc_in);

    search(&ctx, "contents", "apache");
    search(&ctx, "contents", "patent");
    search(&ctx, "contents", "lucene");
    search(&ctx, "contents", "gnu");
    search(&ctx, "contents", "derivative");
    search(&ctx, "contents", "license");

    free_file_buffers(ctx.file_buffers);

    return 0;
}

static block_tree_term_reader_t *build_fields_producer(lucene_global_context_t *ctx) {
    field_infos_t *field_infos = ctx->field_infos;

    file_buffer_t *terms_dict = ctx->file_buffers + LUCENE_FILE_TIM;
    file_buffer_t *terms_index = ctx->file_buffers + LUCENE_FILE_TIP;

    data_input_t *terms_in = allocation_get(sizeof(*terms_in));
    terms_in->index = 0;
    terms_in->buffer = terms_dict->content;
    terms_in->read_byte = incremental_read_byte;
    terms_in->skip_bytes = incremental_skip_bytes;

    data_input_t *index_in = allocation_get(sizeof(*index_in));
    index_in->index = 0;
    index_in->buffer = terms_index->content;
    index_in->read_byte = incremental_read_byte;
    index_in->skip_bytes = incremental_skip_bytes;

    return block_tree_term_reader_new(field_infos, terms_in, (uint32_t) terms_dict->length, index_in,
                                      (uint32_t) terms_index->length);
}

static data_input_t *build_doc_in(lucene_global_context_t *ctx) {
    file_buffer_t *docs = ctx->file_buffers + LUCENE_FILE_DOC;

    data_input_t *doc_in = allocation_get(sizeof(*doc_in));
    doc_in->index = 0;
    doc_in->buffer = docs->content;
    doc_in->read_byte = incremental_read_byte;
    doc_in->skip_bytes = incremental_skip_bytes;

    check_index_header(doc_in);

    return doc_in;
}

static void search(lucene_global_context_t *ctx, char *field, char *value) {
    term_t *term = term_from_string(field, value);

    field_reader_t *terms = get_terms(ctx->term_reader, term->field);
    segment_terms_enum_t *terms_enum = segment_terms_enum_new(terms);
    bool result = seek_exact(terms_enum, term->bytes);

    if (result) {
        block_term_state_t *term_state = get_term_state(terms_enum);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);
        printf("MATCH for '%s' (doc_freq=%d, total_term_freq=%ld)\n", term->bytes->bytes, doc_freq, total_term_freq);

        term_weight_t *weight = build_weight(term, SCORE_MODE_TOP_SCORES, 1.0f, term_state);
        term_scorer_t *scorer = build_scorer(weight, terms_enum, ctx->doc_in, ctx->for_util);

        int32_t doc;
        while ((doc = postings_next_doc(scorer->postings_enum)) != NO_MORE_DOCS) {
            float score = compute_score(terms->doc_count,
                                        (uint32_t) doc_freq,
                                        scorer->postings_enum->freq,
                                        getNorms(ctx->norms_reader, terms->field_info->number, doc),
                                        terms->sum_total_term_freq);
            printf("doc:%d freq:%d score:%f\n", doc, scorer->postings_enum->freq, score);
        }
    } else {
        printf("NO MATCH for '%s'\n", term->bytes->bytes);
    }

}
