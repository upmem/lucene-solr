/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include <stdio.h>
#include "parser.h"
#include "segment_terms_enum.h"
#include "field_infos.h"
#include "term.h"
#include "allocation.h"
#include "norms.h"

typedef struct {
    file_buffer_t *file_buffers;
    block_tree_term_reader_t *term_reader;
    norms_reader_t *norms_reader;
    field_infos_t *field_infos;
} lucene_global_context_t;

static block_tree_term_reader_t* build_fields_producer(lucene_global_context_t *ctx);
static void search(lucene_global_context_t *ctx, char* field, char* value);

static norms_reader_t *build_norms_producer(lucene_global_context_t *ctx)
{
    field_infos_t* field_infos = ctx->field_infos;

    file_buffer_t* buffer_norms_data = ctx->file_buffers + LUCENE_FILE_NVD;
    file_buffer_t* buffer_norms_metadata = ctx->file_buffers + LUCENE_FILE_NVM;

    data_input_t* norms_data = allocation_get(sizeof(*norms_data));
    norms_data->index = 0;
    norms_data->buffer = buffer_norms_data->content;
    norms_data->read_byte = incremental_read_byte;
    norms_data->skip_bytes = incremental_skip_bytes;

    data_input_t* norms_metadata = allocation_get(sizeof(*norms_metadata));
    norms_metadata->index = 0;
    norms_metadata->buffer = buffer_norms_metadata->content;
    norms_metadata->read_byte = incremental_read_byte;
    norms_metadata->skip_bytes = incremental_skip_bytes;

    return norms_reader_new(norms_metadata, buffer_norms_metadata->length,
                            norms_data, buffer_norms_data->length,
                            field_infos);
}

int main(int argc, char **argv)
{
    lucene_global_context_t ctx;
    ctx.file_buffers = get_file_buffers(argv[1], 0);
    ctx.field_infos = read_field_infos(ctx.file_buffers + LUCENE_FILE_FNM);
    ctx.term_reader = build_fields_producer(&ctx);
    ctx.norms_reader = build_norms_producer(&ctx);

    search(&ctx, "contents", "apache");
    search(&ctx, "contents", "patent");
    search(&ctx, "contents", "lucene");
    search(&ctx, "contents", "gnu");
    search(&ctx, "contents", "derivative");
    search(&ctx, "contents", "license");

    free_file_buffers(ctx.file_buffers);

    return 0;
}

static block_tree_term_reader_t* build_fields_producer(lucene_global_context_t *ctx) {
    field_infos_t* field_infos = ctx->field_infos;

    file_buffer_t* terms_dict = ctx->file_buffers + LUCENE_FILE_TIM;
    file_buffer_t* terms_index = ctx->file_buffers + LUCENE_FILE_TIP;

    data_input_t* terms_in = allocation_get(sizeof(*terms_in));
    terms_in->index = 0;
    terms_in->buffer = terms_dict->content;
    terms_in->read_byte = incremental_read_byte;
    terms_in->skip_bytes = incremental_skip_bytes;

    data_input_t* index_in = allocation_get(sizeof(*index_in));
    index_in->index = 0;
    index_in->buffer = terms_index->content;
    index_in->read_byte = incremental_read_byte;
    index_in->skip_bytes = incremental_skip_bytes;

    return block_tree_term_reader_new(field_infos, terms_in, (uint32_t) terms_dict->length, index_in, (uint32_t) terms_index->length);
}

static void search(lucene_global_context_t *ctx, char* field, char* value) {
    term_t* term = term_from_string(field, value);

    field_reader_t* terms = get_terms(ctx->term_reader, term->field);
    segment_terms_enum_t* terms_enum = segment_terms_enum_new(terms);
    bool result = seek_exact(terms_enum, term->bytes);

    if (result) {
        block_term_state_t* term_state = get_term_state(terms_enum);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);
        printf("MATCH for '%s' (doc_freq=%d, total_term_freq=%ld)\n", term->bytes->bytes, doc_freq, total_term_freq);
    } else {
        printf("NO MATCH for '%s'\n", term->bytes->bytes);
    }

    int doc_id = 8;
    printf("Norms: %llu doc: %i\n", getNorms(ctx->norms_reader, terms->field_info->number, doc_id), doc_id);
    doc_id = 10;
    printf("Norms: %llu doc: %i\n", getNorms(ctx->norms_reader, terms->field_info->number, doc_id), doc_id);
    doc_id = 12;
    printf("Norms: %llu doc: %i\n", getNorms(ctx->norms_reader, terms->field_info->number, doc_id), doc_id);
}
