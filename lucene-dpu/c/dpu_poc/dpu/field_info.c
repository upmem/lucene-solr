/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>
#include "field_info.h"
#include "mram_reader.h"
#include "alloc_wrapper.h"
#include "index_header.h"

#define FORMAT_SELECTIVE_INDEXING 2

// Field flags
#define STORE_TERMVECTOR        0x1
#define OMIT_NORMS              0x2
#define STORE_PAYLOADS          0x4
#define SOFT_DELETES_FIELD      0x8

static field_infos_t *field_infos_new(field_info_t *infos, uint32_t infos_length);

static index_options_t get_index_options(uint8_t b);

static doc_values_type_t get_doc_values_types(uint8_t b);

static void fill_field_info(field_info_t *info,
                            char *name,
                            uint32_t number,
                            doc_values_type_t doc_values_type,
                            bool store_term_vector,
                            bool omit_norms,
                            index_options_t index_options,
                            bool store_payloads,
                            string_map_t *attributes,
                            int64_t dv_gen,
                            int32_t point_data_dimension_count,
                            int32_t point_index_dimension_count,
                            int32_t point_num_bytes,
                            bool soft_deletes_field);

int32_t compare_index_options(index_options_t first, index_options_t second) {
    return ((int32_t) first) - ((int32_t) second);
}

field_infos_t *read_field_infos(file_buffer_t *file) {
    mram_reader_t _input = {
            .index = file->offset,
            .base = file->offset,
            .cache = mram_cache_for(me())
    };
    mram_reader_t *input = &_input;

    int32_t version = check_index_header(input);
    uint32_t size = mram_read_vint(input, false);
    field_info_t *infos = malloc(size * sizeof(*infos));

    for (int i = 0; i < size; ++i) {
        uint32_t name_length;
        char *name = mram_read_string(input, &name_length, false);
        uint32_t field_number = mram_read_vint(input, false);
        uint8_t bits = mram_read_byte(input, false);

        bool store_term_vector = (bits & STORE_TERMVECTOR) != 0;
        bool omit_norms = (bits & OMIT_NORMS) != 0;
        bool store_payloads = (bits & STORE_PAYLOADS) != 0;
        bool is_soft_deletes_field = (bits & SOFT_DELETES_FIELD) != 0;

        index_options_t index_options = get_index_options(mram_read_byte(input, false));

        doc_values_type_t doc_values_type = get_doc_values_types(mram_read_byte(input, false));
        int64_t dv_gen = mram_read_long(input, false);
        string_map_t *attributes = mram_read_map_of_strings(input, false);
        int32_t point_data_dimension_count = mram_read_vint(input, false);
        int32_t point_num_bytes;
        int32_t point_index_dimension_count = point_data_dimension_count;
        if (point_data_dimension_count != 0) {
            if (version >= FORMAT_SELECTIVE_INDEXING) {
                point_index_dimension_count = mram_read_vint(input, false);
            }
            point_num_bytes = mram_read_vint(input, false);
        } else {
            point_num_bytes = 0;
        }

        fill_field_info(infos + i, name, field_number, doc_values_type, store_term_vector, omit_norms, index_options,
                        store_payloads, attributes, dv_gen, point_data_dimension_count, point_index_dimension_count,
                        point_num_bytes,
                        is_soft_deletes_field);
    }

    return field_infos_new(infos, size);
}

static field_infos_t *field_infos_new(field_info_t *infos, uint32_t infos_length) {
    field_infos_t *result = malloc(sizeof(*result));

    result->has_vectors = false;
    result->has_prox = false;
    result->has_payloads = false;
    result->has_offsets = false;
    result->has_freq = false;
    result->has_norms = false;
    result->has_doc_values = false;
    result->has_point_values = false;
    result->soft_deletes_field = NULL;

    uint32_t max_number = 0;
    for (int i = 0; i < infos_length; ++i) {
        if (infos[i].number > max_number) {
            max_number = infos[i].number;
        }
    }

    result->by_number_length = max_number + 1;
    result->by_number = malloc(result->by_number_length * sizeof(*(result->by_number)));

    for (int i = 0; i < infos_length; ++i) {
        field_info_t *info = infos + i;

        result->has_vectors |= info->store_term_vector;
        result->has_prox |= compare_index_options(info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        result->has_freq |= info->index_options != INDEX_OPTIONS_DOCS;
        result->has_offsets |=
                compare_index_options(info->index_options, INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        result->has_norms |= (info->index_options != INDEX_OPTIONS_NONE) && !info->omit_norms;
        result->has_doc_values |= info->doc_values_type != DOC_VALUES_TYPE_NONE;
        result->has_payloads |= info->store_payloads;
        result->has_point_values |= info->point_data_dimension_count != 0;
        if (info->soft_deletes_field) {
            result->soft_deletes_field = info->name;
        }

        result->by_number[info->number] = info;
    }

    return result;
}

static index_options_t get_index_options(uint8_t b) {
    switch (b) {
        case 0:
            return INDEX_OPTIONS_NONE;
        case 1:
            return INDEX_OPTIONS_DOCS;
        case 2:
            return INDEX_OPTIONS_DOCS_AND_FREQS;
        case 3:
            return INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS;
        case 4:
            return INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        default:
            // BUG
            abort();
    }
}

static doc_values_type_t get_doc_values_types(uint8_t b) {
    switch (b) {
        case 0:
            return DOC_VALUES_TYPE_NONE;
        case 1:
            return DOC_VALUES_TYPE_NUMERIC;
        case 2:
            return DOC_VALUES_TYPE_BINARY;
        case 3:
            return DOC_VALUES_TYPE_SORTED;
        case 4:
            return DOC_VALUES_TYPE_SORTED_SET;
        case 5:
            return DOC_VALUES_TYPE_SORTED_NUMERIC;
        default:
            // BUG
            abort();
    }
}

static void fill_field_info(field_info_t *info,
                            char *name,
                            uint32_t number,
                            doc_values_type_t doc_values_type,
                            bool store_term_vector,
                            bool omit_norms,
                            index_options_t index_options,
                            bool store_payloads,
                            string_map_t *attributes,
                            int64_t dv_gen,
                            int32_t point_data_dimension_count,
                            int32_t point_index_dimension_count,
                            int32_t point_num_bytes,
                            bool soft_deletes_field) {
    info->name = name;
    info->number = number;
    info->doc_values_type = doc_values_type;
    info->index_options = index_options;

    if (index_options != INDEX_OPTIONS_NONE) {
        info->store_term_vector = store_term_vector;
        info->store_payloads = store_payloads;
        info->omit_norms = omit_norms;
    } else {
        info->store_term_vector = false;
        info->store_payloads = false;
        info->omit_norms = false;
    }
    info->dv_gen = dv_gen;
    info->attributes = attributes;
    info->point_data_dimension_count = point_data_dimension_count;
    info->point_index_dimension_count = point_index_dimension_count;
    info->point_num_bytes = point_num_bytes;
    info->soft_deletes_field = soft_deletes_field;
}