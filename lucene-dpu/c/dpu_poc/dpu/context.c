/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>
#include <string.h>

#include <defs.h>

#include "norms.h"
#include "context.h"
#include "mram_access.h"
#include "mram_structure.h"
#include "dpu_characteristics.h"
#include "alloc_wrapper.h"
#include "terms_enum_frame.h"

query_t __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) the_query;

search_context_t contexts[NR_THREADS];
terms_enum_t terms_enums[NR_THREADS];

#define MAX_FILE_EXTENSION_SIZE 5

static const char *lucene_file_extension[LUCENE_FILE_ENUM_LENGTH] =
        {
                [LUCENE_FILE_SI] = ".si",
                [LUCENE_FILE_CFE] = ".cfe",
                [LUCENE_FILE_CFS] = ".cfs",
                [LUCENE_FILE_FNM] = ".fnm",
                [LUCENE_FILE_FDX] = ".fdx",
                [LUCENE_FILE_FDT] = ".fdt",
                [LUCENE_FILE_TIM] = ".tim",
                [LUCENE_FILE_TIP] = ".tip",
                [LUCENE_FILE_DOC] = ".doc",
                [LUCENE_FILE_POS] = ".pos",
                [LUCENE_FILE_PAY] = ".pay",
                [LUCENE_FILE_NVD] = ".nvd",
                [LUCENE_FILE_NVM] = ".nvm",
                [LUCENE_FILE_DVD] = ".dvd",
                [LUCENE_FILE_DVM] = ".dvm",
                [LUCENE_FILE_TVX] = ".tvx",
                [LUCENE_FILE_TVD] = ".tvd",
                [LUCENE_FILE_LIV] = ".liv",
                [LUCENE_FILE_DII] = ".dii",
                [LUCENE_FILE_DIM] = ".dim",
        };

static void generate_file_buffers_from_cfs(mram_addr_t cfe_offset, mram_addr_t cfs_offset, file_buffer_t *file_buffers);
static lucene_file_e extension_to_index(uint8_t *file_name);
static void build_fields_producer(term_reader_t *term_reader, search_context_t *ctx);

static void build_doc_reader(mram_reader_t *doc_reader, file_buffer_t *file_buffers) {
    file_buffer_t *docs = file_buffers + LUCENE_FILE_DOC;

    doc_reader->index = docs->offset;
    doc_reader->base = docs->offset;
    doc_reader->cache = mram_cache_for(me());

    check_index_header(doc_reader);
}

static norms_reader_t *build_norms_reader(search_context_t *ctx) {
    field_infos_t *field_infos = &ctx->field_infos;

    file_buffer_t *buffer_norms_data = ctx->file_buffers + LUCENE_FILE_NVD;
    file_buffer_t *buffer_norms_metadata = ctx->file_buffers + LUCENE_FILE_NVM;

    return norms_reader_new(buffer_norms_metadata, buffer_norms_data, field_infos);
}

search_context_t *initialize_context(uint32_t index) {
    search_context_t* context = contexts + index;

    uint32_t __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) offsets[NR_FILES_OFFSETS];

    read_files_offsets(index, offsets);

    mram_addr_t cfe_offset = offsets[1];
    mram_addr_t cfs_offset = offsets[2];

    generate_file_buffers_from_cfs(cfe_offset, cfs_offset, context->file_buffers);

    read_field_infos(&context->field_infos, context->file_buffers + LUCENE_FILE_FNM);
    build_fields_producer(&context->term_reader, context);
    build_doc_reader(&context->doc_reader, context->file_buffers);
    context->for_util = build_for_util(&context->doc_reader, index);
    context->norms_reader = build_norms_reader(context);

    return context;
}

query_t *fetch_query(bool do_init) {
    if (do_init) {
        read_query(&the_query);
    }

    return &the_query;
}

terms_enum_t *initialize_terms_enum(uint32_t index, field_reader_t *field_reader) {
    terms_enum_t *terms_enum = terms_enums + index;

    terms_enum->field_reader = field_reader;
    terms_enum->in_initialized = false;
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

static void generate_file_buffers_from_cfs(mram_addr_t cfe_offset, mram_addr_t cfs_offset, file_buffer_t *file_buffers) {
    // parse_cfe merged to avoid allocation
    mram_reader_t buffer = {
            .index = cfe_offset,
            .base = cfe_offset,
            .cache = mram_cache_for(me())
    };
    unsigned int each;
    uint32_t unused_length;

    index_header_t index_header;
    read_index_header(&index_header, &buffer);

    uint32_t num_entries = mram_read_vint(&buffer, false);

    for (each = 0; each < num_entries; each++) {
        char entry_name[MAX_FILE_EXTENSION_SIZE];
        mram_read_string(&buffer, entry_name, false);
        uint32_t offset = mram_read_false_long(&buffer, false);
        uint32_t length = mram_read_false_long(&buffer, false);

        unsigned int file_buffers_id = extension_to_index((uint8_t*)entry_name);
        file_buffers[file_buffers_id].offset = cfs_offset + offset;
        file_buffers[file_buffers_id].length = length;
    }
}

static lucene_file_e extension_to_index(uint8_t *file_name) {
    unsigned int each;
    for (each = 0; each < LUCENE_FILE_ENUM_LENGTH; each++) {
        size_t file_name_size = strlen((const char *)file_name);
        if (!strncmp((const char *)&file_name[file_name_size - 3], &lucene_file_extension[each][1], 3))
            return each;
    }
    halt();
}


static void build_fields_producer(term_reader_t *term_reader, search_context_t *ctx) {
    field_infos_t *field_infos = &ctx->field_infos;
    file_buffer_t *terms_dict = ctx->file_buffers + LUCENE_FILE_TIM;
    file_buffer_t *terms_index = ctx->file_buffers + LUCENE_FILE_TIP;

    term_reader_new(term_reader, field_infos, terms_dict, terms_index);
}
