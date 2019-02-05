/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FIELD_INFO_H
#define DPU_POC_FIELD_INFO_H

#include <stdint.h>
#include <stdbool.h>
#include "file.h"
#include "string_map.h"

typedef enum {
    DOC_VALUES_TYPE_NONE,
    DOC_VALUES_TYPE_NUMERIC,
    DOC_VALUES_TYPE_BINARY,
    DOC_VALUES_TYPE_SORTED,
    DOC_VALUES_TYPE_SORTED_NUMERIC,
    DOC_VALUES_TYPE_SORTED_SET
} doc_values_type_t;

typedef enum {
    INDEX_OPTIONS_NONE,
    INDEX_OPTIONS_DOCS,
    INDEX_OPTIONS_DOCS_AND_FREQS,
    INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS,
    INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
} index_options_t;

typedef struct {
    char *name;
    uint32_t number;
    doc_values_type_t doc_values_type;
    bool store_term_vector;
    bool omit_norms;
    index_options_t index_options;
    bool store_payloads;
    string_map_t *attributes;
    int64_t dv_gen;
    int32_t point_data_dimension_count;
    int32_t point_index_dimension_count;
    int32_t point_num_bytes;
    bool soft_deletes_field;
} field_info_t;

typedef struct {
    bool has_freq;
    bool has_prox;
    bool has_payloads;
    bool has_offsets;
    bool has_vectors;
    bool has_norms;
    bool has_doc_values;
    bool has_point_values;
    char *soft_deletes_field;

    field_info_t **by_number;
    uint32_t by_number_length;
} field_infos_t;

int32_t compare_index_options(index_options_t first, index_options_t second);

field_infos_t *read_field_infos(file_buffer_t *file);

#endif //DPU_POC_FIELD_INFO_H
