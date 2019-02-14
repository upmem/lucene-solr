#include <elf.h>

/*
 * Copyright (c) 2014-2019 - uPmem
 */

typedef uint32_t mram_addr_t;
typedef void mram_cache_t;

#include <stdlib.h>
#include <stdio.h>
#include "x86_process/lib.h"
#include <search_context.h>
#include <field_reader_struct.h>
#include <string.h>
#include <mram_structure.h>
#include "segment_files.h"

mram_image_t *mram_image_new() {
    mram_image_t *image = malloc(sizeof(*image));

    image->content = malloc(MRAM_SIZE);
    image->nr_segments = 0;
    image->current_offset = SEGMENTS_OFFSET;
    memset(image->content + QUERY_BUFFER_OFFSET, 0, QUERY_BUFFER_SIZE);

    return image;
}

void free_mram_image(mram_image_t *mram_image) {
    free(mram_image->content);
    free(mram_image);
}

bool load_segment_files(mram_image_t *mram_image, const char* index_directory, uint32_t segment_number) {
    if (mram_image->nr_segments == NR_THREADS) {
        return false;
    }

    uint32_t offset_address = SEGMENT_SUMMARY_OFFSET + mram_image->nr_segments * SEGMENT_SUMMARY_ENTRY_SIZE;
    *((uint64_t *)(mram_image->content + offset_address)) = (mram_image->current_offset & 0xffffffffl) | (((uint64_t) segment_number) << 32);

    lucene_global_context_t *global_context = fetch_lucene_global_context((char *) index_directory, segment_number);
    flat_search_context_t *search_context = (flat_search_context_t *) (mram_image->content + mram_image->current_offset);
    mram_image->current_offset += sizeof(*search_context);

    search_context->term_reader.nr_fields = global_context->term_reader->nr_fields;

    search_context->field_infos.has_freq = global_context->field_infos->has_freq;
    search_context->field_infos.has_prox = global_context->field_infos->has_prox;
    search_context->field_infos.has_payloads = global_context->field_infos->has_payloads;
    search_context->field_infos.has_offsets = global_context->field_infos->has_offsets;
    search_context->field_infos.has_vectors = global_context->field_infos->has_vectors;
    search_context->field_infos.has_norms = global_context->field_infos->has_norms;
    search_context->field_infos.has_doc_values = global_context->field_infos->has_doc_values;
    search_context->field_infos.has_point_values = global_context->field_infos->has_point_values;
    if (global_context->field_infos->soft_deletes_field != NULL) {
        strcpy(search_context->field_infos.soft_deletes_field, global_context->field_infos->soft_deletes_field);
    } else {
        search_context->field_infos.soft_deletes_field[0] = '\0';
    }

    search_context->nr_norms_entries = global_context->field_infos->by_number_length;

    memcpy(search_context->for_util.setup_done, global_context->for_util->setup_done, sizeof(search_context->for_util.setup_done));
    memcpy(search_context->for_util.encoded_sizes, global_context->for_util->encoded_sizes, sizeof(search_context->for_util.encoded_sizes));
    memcpy(search_context->for_util.decoders, global_context->for_util->decoders, sizeof(search_context->for_util.decoders));
    memcpy(search_context->for_util.iterations, global_context->for_util->iterations, sizeof(search_context->for_util.iterations));

    mram_image->current_offset = DMA_ALIGNED(mram_image->current_offset);

    for (int i = 0; i < global_context->field_infos->by_number_length; ++i) {
        if (global_context->norms_reader->norms_entries[i] != NULL) {
            memcpy(mram_image->content + mram_image->current_offset, global_context->norms_reader->norms_entries[i],
                   sizeof(norms_entry_t));
        }
        mram_image->current_offset += DMA_ALIGNED(sizeof(norms_entry_t));
    }

    uint32_t empty_outputs_offset_start = mram_image->current_offset + global_context->term_reader->nr_fields * DMA_ALIGNED(sizeof(flat_field_reader_t));
    uint32_t empty_outputs_offset = empty_outputs_offset_start;
    uint32_t empty_output_length = 0;

    for (int j = 0; j < global_context->term_reader->nr_fields; ++j) {
        empty_output_length += sizeof(uint32_t) + ((global_context->term_reader->fields[j].field_reader->index->empty_output->length + 3) & ~3);
    }

    search_context->empty_outputs_length = empty_output_length;

    uint32_t fst_contents_offset = empty_outputs_offset + empty_output_length;

    for (int j = 0; j < global_context->term_reader->nr_fields; ++j) {
        field_reader_t* field_reader = global_context->term_reader->fields[j].field_reader;
        flat_field_reader_t flat_reader;

        flat_reader.longs_size = field_reader->longs_size;
        flat_reader.doc_count = field_reader->doc_count;
        flat_reader.sum_total_term_freq = field_reader->sum_total_term_freq;

        flat_reader.index.empty_output_offset = empty_outputs_offset - empty_outputs_offset_start;
        flat_reader.index.empty_output = DPU_NULL;
        flat_reader.index.start_node = field_reader->index->start_node;
        flat_reader.index.input_type = field_reader->index->input_type;
        flat_reader.index.mram_start_offset = fst_contents_offset;
        flat_reader.index.mram_length = field_reader->index->bytes_array_length;

        strcpy(flat_reader.field_info.name, field_reader->field_info->name);
        flat_reader.field_info.number = field_reader->field_info->number;
        flat_reader.field_info.doc_values_type = field_reader->field_info->doc_values_type;
        flat_reader.field_info.store_term_vector = field_reader->field_info->store_term_vector;
        flat_reader.field_info.omit_norms = field_reader->field_info->omit_norms;
        flat_reader.field_info.index_options = field_reader->field_info->index_options;
        flat_reader.field_info.store_payloads = field_reader->field_info->store_payloads;
        flat_reader.field_info.dv_gen = field_reader->field_info->dv_gen;
        flat_reader.field_info.point_data_dimension_count = field_reader->field_info->point_data_dimension_count;
        flat_reader.field_info.point_index_dimension_count = field_reader->field_info->point_index_dimension_count;
        flat_reader.field_info.point_num_bytes = field_reader->field_info->point_num_bytes;
        flat_reader.field_info.soft_deletes_field = field_reader->field_info->soft_deletes_field;

        memcpy(mram_image->content + fst_contents_offset, field_reader->index->bytes_array, field_reader->index->bytes_array_length);
        fst_contents_offset += field_reader->index->bytes_array_length;

        *((uint32_t*) (mram_image->content + empty_outputs_offset)) = field_reader->index->empty_output->length;
        memcpy(mram_image->content + empty_outputs_offset + sizeof(uint32_t), field_reader->index->empty_output->bytes, field_reader->index->empty_output->length);
        empty_outputs_offset += sizeof(uint32_t) + ((field_reader->index->empty_output->length + 3) & ~3);
        memcpy(mram_image->content + mram_image->current_offset, &flat_reader, sizeof(flat_reader));
        mram_image->current_offset += DMA_ALIGNED(sizeof(flat_field_reader_t));
    }

    mram_image->current_offset = DMA_ALIGNED(fst_contents_offset);

    search_context->fields = DPU_NULL;
    search_context->entries = DPU_NULL;

    memcpy(mram_image->content + mram_image->current_offset, global_context->term_reader->terms_in->buffer, global_context->term_reader->terms_in->size);
    search_context->term_reader.terms_in.index = mram_image->current_offset;
    search_context->term_reader.terms_in.base = mram_image->current_offset;
    search_context->term_reader.terms_in.cache = DPU_NULL;
    mram_image->current_offset += DMA_ALIGNED(global_context->term_reader->terms_in->size);

    memcpy(mram_image->content + mram_image->current_offset, global_context->doc_in->buffer, global_context->doc_in->size);
    search_context->doc_reader.index = mram_image->current_offset;
    search_context->doc_reader.base = mram_image->current_offset;
    search_context->doc_reader.cache = DPU_NULL;
    mram_image->current_offset += DMA_ALIGNED(global_context->doc_in->size);

    memcpy(mram_image->content + mram_image->current_offset, global_context->norms_reader->norms_data->buffer, global_context->norms_reader->norms_data->size);
    search_context->norms_data.index = mram_image->current_offset;
    search_context->norms_data.base = mram_image->current_offset;
    search_context->norms_data.cache = DPU_NULL;
    mram_image->current_offset += DMA_ALIGNED(global_context->norms_reader->norms_data->size);

    mram_image->nr_segments++;

    return true;
}