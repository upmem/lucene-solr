/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include <string.h>

#include <defs.h>

#include "context.h"
#include "mram_access.h"
#include "../commons/mram_structure.h"
#include "../commons/dpu_characteristics.h"
#include "segments_info.h"
#include "cfe.h"
#include "alloc_wrapper.h"

query_t __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) the_query;

search_context_t contexts[NR_THREADS];
terms_enum_t terms_enums[NR_THREADS];

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
static term_reader_t *build_fields_producer(search_context_t *ctx);

static mram_reader_t *build_doc_reader(file_buffer_t *file_buffers) {
    file_buffer_t *docs = file_buffers + LUCENE_FILE_DOC;

    mram_reader_t *doc_reader = malloc(sizeof(*doc_reader));
    doc_reader->index = 0;
    doc_reader->base = docs->offset;
    doc_reader->cache = mram_cache_for(me());

    check_index_header(doc_reader);
    return doc_reader;
}

search_context_t *initialize_context(uint32_t index) {
    search_context_t* context = contexts + index;

    uint32_t __attribute__((aligned(MRAM_ACCESS_ALIGNMENT))) offsets[NR_FILES_OFFSETS];

    read_files_offsets(index, offsets);

    mram_addr_t si_offset = offsets[0];
    mram_addr_t cfe_offset = offsets[1];
    mram_addr_t cfs_offset = offsets[2];

    segments_info_t *segments_info = parse_segments_info(si_offset);

    if (segments_info->IsCompoundFile) {
        generate_file_buffers_from_cfs(cfe_offset, cfs_offset, context->file_buffers);
    } else {
        abort();
    }

    context->field_infos = read_field_infos(context->file_buffers + LUCENE_FILE_FNM);
    context->term_reader = build_fields_producer(context);
    context->doc_reader = build_doc_reader(context->file_buffers);
    context->for_util = build_for_util(context->doc_reader);

    return context;
}

query_t *fetch_query(bool do_init) {
    if (do_init) {
        read_query(&the_query);
    }

    return &the_query;
}

terms_enum_t *initialize_terms_enum(uint32_t index) {
    terms_enum_t *terms_enum = terms_enums + index;

    terms_enum->in = NULL;
    terms_enum->stack = NULL;
    terms_enum->stack_length = 0;
    terms_enum->static_frame = term_enum_frame_new(terms_enum, -1);

    terms_enum->arcs = malloc(sizeof(*(terms_enum->arcs)));
    memset(terms_enum->arcs, 0, sizeof(*(terms_enum->arcs)));
    terms_enum->arcs_length = 1;

    terms_enum->current_frame = terms_enum->static_frame;

    terms_enum->valid_index_prefix = 0;
    terms_enum->term = bytes_ref_new();

    return terms_enum;
}

static void generate_file_buffers_from_cfs(mram_addr_t cfe_offset, mram_addr_t cfs_offset, file_buffer_t *file_buffers) {
    cfe_info_t *cfe_info = parse_cfe_info(cfe_offset);

    for (uint32_t each = 0; each < cfe_info->num_entries; each++) {
        unsigned int file_buffers_id = extension_to_index(cfe_info->entries[each].entry_name);
        file_buffers[file_buffers_id].offset = cfs_offset + cfe_info->entries[each].offset;
        file_buffers[file_buffers_id].length = cfe_info->entries[each].length;
    }
}

static lucene_file_e extension_to_index(uint8_t *file_name) {
    unsigned int each;
    for (each = 0; each < LUCENE_FILE_ENUM_LENGTH; each++) {
        size_t file_name_size = strlen((const char *)file_name);
        if (!strncmp((const char *)&file_name[file_name_size - 3], &lucene_file_extension[each][1], 3))
            return each;
    }
    abort();
}


static term_reader_t *build_fields_producer(search_context_t *ctx) {
    field_infos_t *field_infos = ctx->field_infos;

    file_buffer_t *terms_dict = ctx->file_buffers + LUCENE_FILE_TIM;
    file_buffer_t *terms_index = ctx->file_buffers + LUCENE_FILE_TIP;

    mram_reader_t *terms_in = malloc(sizeof(*terms_in));
    terms_in->index = terms_dict->offset;
    terms_in->base = terms_dict->offset;

    mram_reader_t *index_in = malloc(sizeof(*index_in));
    index_in->index = terms_index->offset;
    index_in->base = terms_index->offset;

    return term_reader_new(field_infos, terms_in, (uint32_t) terms_dict->length, index_in, (uint32_t) terms_index->length);
}
