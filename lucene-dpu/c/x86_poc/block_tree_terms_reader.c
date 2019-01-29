/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>

#include "block_tree_terms_reader.h"
#include "allocation.h"
#include "parser_codec_footer.h"

typedef struct {
    uint32_t field;
    uint64_t num_terms;
    bytes_ref_t* root_code;
    uint64_t sum_total_term_freq;
    uint64_t sum_doc_freq;
    uint32_t doc_count;
    uint32_t longs_size;
    bytes_ref_t* min_term;
    bytes_ref_t* max_term;
} field_details_t;

typedef struct {
    uint32_t num_fields;
    field_details_t* field_details;
} fields_details_t;

static void seek_dir(data_input_t* in, uint32_t in_length);
static fields_details_t* parse_fields_details(field_infos_t* field_infos, data_input_t* terms_in, uint32_t terms_in_length);
static bytes_ref_t* read_bytes_ref(data_input_t* terms_in);
static field_reader_t* field_reader_new(block_tree_term_reader_t* parent, field_info_t* field_info, uint64_t index_start_fp, uint32_t longs_size, data_input_t* index_in);

static field_reader_t* field_reader_new(block_tree_term_reader_t* parent, field_info_t* field_info, uint64_t index_start_fp, uint32_t longs_size, data_input_t* index_in) {
    field_reader_t* reader = allocation_get(sizeof(*reader));

    reader->field_info = field_info;
    reader->longs_size = longs_size;
    reader->parent = parent;
    // todo other fields if needed

    if (index_in != NULL) {
        data_input_t* clone = data_input_clone(index_in);
        clone->index = (uint32_t) index_start_fp;
        reader->index = fst_new(clone);
    } else {
        reader->index = NULL;
    }

    return reader;
}

block_tree_term_reader_t* block_tree_term_reader_new(field_infos_t* field_infos,
        data_input_t* terms_in, uint32_t terms_in_length, data_input_t* input_in, uint32_t input_in_length) {
    // todo: Lucene parses both files at the same time

    fields_details_t* details = parse_fields_details(field_infos, terms_in, terms_in_length);

    seek_dir(input_in, input_in_length);

    block_tree_term_reader_t* reader = allocation_get(sizeof(*reader));
    reader->terms_in = terms_in;
    reader->nr_fields = details->num_fields;
    reader->fields = allocation_get(reader->nr_fields * sizeof(*(reader->fields)));

    for (int i = 0; i < reader->nr_fields; ++i) {
        field_details_t* field_detail = details->field_details + i;
        block_tree_term_reader_field_t* field = reader->fields + i;

        uint64_t index_start_fp = read_vlong(input_in);
        field_info_t* field_info = field_infos->by_number[field_detail->field];

        field->name = field_info->name;
        field->field_reader = field_reader_new(reader, field_info, index_start_fp, field_detail->longs_size, input_in);
    }

    return reader;
}

field_reader_t* get_terms(block_tree_term_reader_t* reader, const char* field) {
    for (int i = 0; i < reader->nr_fields; ++i) {
        block_tree_term_reader_field_t* reader_field = reader->fields + i;
        if (strcmp(reader_field->name, field) == 0) {
            return reader_field->field_reader;
        }
    }

    return NULL;
}

static fields_details_t* parse_fields_details(field_infos_t* field_infos, data_input_t* terms_in, uint32_t terms_in_length) {
    fields_details_t* result = allocation_get(sizeof(*result));

    // todo check header

    seek_dir(terms_in, terms_in_length);

    result->num_fields = read_vint(terms_in);
    result->field_details = allocation_get(result->num_fields * sizeof(*(result->field_details)));

    for (int i = 0; i < result->num_fields; ++i) {
        field_details_t* field = result->field_details + i;

        field->field = read_vint(terms_in);
        field->num_terms = read_vlong(terms_in);
        field->root_code = read_bytes_ref(terms_in);

        field_info_t* field_info = field_infos->by_number[field->field];

        field->sum_total_term_freq = read_vlong(terms_in);
        field->sum_doc_freq = (field_info->index_options == INDEX_OPTIONS_DOCS) ? field->sum_total_term_freq : read_vlong(terms_in);
        field->doc_count = read_vint(terms_in);
        field->longs_size = read_vint(terms_in);
        field->min_term = read_bytes_ref(terms_in);
        field->max_term = read_bytes_ref(terms_in);
    }

    return result;
}

static void seek_dir(data_input_t* in, uint32_t in_length) {
    in->index = in_length - sizeof(lucene_codec_footer_t) - 8;
    uint64_t offset = read_long(in);
    in->index = (uint32_t) offset;
}

static bytes_ref_t* read_bytes_ref(data_input_t* terms_in) {
    bytes_ref_t* result = allocation_get(sizeof(*result));

    uint32_t num_bytes = read_vint(terms_in);
    result->bytes = allocation_get(num_bytes);
    result->length = num_bytes;
    result->capacity = num_bytes;
    result->offset = 0;

    read_bytes(terms_in, result->bytes, 0, num_bytes);

    return result;
}