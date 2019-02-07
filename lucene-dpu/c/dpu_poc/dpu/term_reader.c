/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stddef.h>
#include <devprivate.h>
#include <string.h>

#include "term_reader.h"
#include "alloc_wrapper.h"
#include "codec_footer.h"

typedef struct {
    uint32_t field;
    uint64_t num_terms;
    bytes_ref_t *root_code;
    uint64_t sum_total_term_freq;
    uint64_t sum_doc_freq;
    uint32_t doc_count;
    uint32_t longs_size;
    bytes_ref_t *min_term;
    bytes_ref_t *max_term;
} field_details_t;

typedef struct {
    uint32_t num_fields;
    field_details_t *field_details;
} fields_details_t;

static bytes_ref_t *read_bytes_ref(mram_reader_t *terms_in);

static void seek_dir(mram_reader_t *in, uint32_t in_length);

static fields_details_t * parse_fields_details(field_infos_t *field_infos, mram_reader_t *terms_in, uint32_t terms_in_length);

static field_reader_t *field_reader_new(term_reader_t *parent,
                                        field_info_t *field_info,
                                        uint64_t index_start_fp,
                                        uint32_t longs_size,
                                        uint32_t doc_count,
                                        uint64_t sum_total_term_freq,
                                        mram_reader_t *index_in);

field_reader_t *fetch_field_reader(term_reader_t *reader, const char *field) {
    for (int i = 0; i < reader->nr_fields; ++i) {
        term_reader_field_t *reader_field = reader->fields + i;
        if (strcmp(reader_field->name, field) == 0) {
            return reader_field->field_reader;
        }
    }

    return NULL;
}

term_reader_t * term_reader_new(field_infos_t *field_infos, mram_reader_t *terms_in, uint32_t terms_in_length,
                                mram_reader_t *input_in, uint32_t input_in_length) {
    // todo: Lucene parses both files at the same time

    fields_details_t *details = parse_fields_details(field_infos, terms_in, terms_in_length);

    seek_dir(input_in, input_in_length);

    term_reader_t *reader = malloc(sizeof(*reader));
    reader->terms_in = terms_in;
    reader->nr_fields = details->num_fields;
    reader->fields = malloc(reader->nr_fields * sizeof(*(reader->fields)));

    for (int i = 0; i < reader->nr_fields; ++i) {
        field_details_t *field_detail = details->field_details + i;
        term_reader_field_t *field = reader->fields + i;

        uint64_t index_start_fp = mram_read_vlong(input_in, false);
        field_info_t *field_info = field_infos->by_number[field_detail->field];

        field->name = field_info->name;
        field->field_reader = field_reader_new(reader,
                                               field_info,
                                               index_start_fp,
                                               field_detail->longs_size,
                                               field_detail->doc_count,
                                               field_detail->sum_total_term_freq,
                                               input_in);
    }

    return reader;
}

static fields_details_t *parse_fields_details(field_infos_t *field_infos, mram_reader_t *terms_in, uint32_t terms_in_length) {
    fields_details_t *result = malloc(sizeof(*result));

    // todo check header

    seek_dir(terms_in, terms_in_length);

    result->num_fields = mram_read_vint(terms_in, false);
    result->field_details = malloc(result->num_fields * sizeof(*(result->field_details)));

    for (int i = 0; i < result->num_fields; ++i) {
        field_details_t *field = result->field_details + i;

        field->field = mram_read_vint(terms_in, false);
        field->num_terms = mram_read_vlong(terms_in, false);
        field->root_code = read_bytes_ref(terms_in);

        field_info_t *field_info = field_infos->by_number[field->field];

        field->sum_total_term_freq = mram_read_vlong(terms_in, false);
        field->sum_doc_freq = (field_info->index_options == INDEX_OPTIONS_DOCS) ? field->sum_total_term_freq
                                                                                : mram_read_vlong(terms_in, false);
        field->doc_count = mram_read_vint(terms_in, false);
        field->longs_size = mram_read_vint(terms_in, false);
        field->min_term = read_bytes_ref(terms_in);
        field->max_term = read_bytes_ref(terms_in);
    }

    return result;
}

static void seek_dir(mram_reader_t *in, uint32_t in_length) {
    set_index(in, in_length - sizeof(codec_footer_t) - 8);
    uint64_t offset = mram_read_long(in, false);
    set_index(in, (uint32_t) offset);
}

static bytes_ref_t *read_bytes_ref(mram_reader_t *terms_in) {
    bytes_ref_t *result = malloc(sizeof(*result));

    uint32_t num_bytes = mram_read_vint(terms_in, false);
    result->bytes = malloc(num_bytes);
    result->length = num_bytes;
    result->capacity = num_bytes;
    result->offset = 0;

    mram_read_bytes(terms_in, result->bytes, 0, num_bytes, false);

    return result;
}

static field_reader_t *field_reader_new(term_reader_t *parent,
                                        field_info_t *field_info,
                                        uint64_t index_start_fp,
                                        uint32_t longs_size,
                                        uint32_t doc_count,
                                        uint64_t sum_total_term_freq,
                                        mram_reader_t *index_in) {
    field_reader_t *reader = malloc(sizeof(*reader));

    reader->field_info = field_info;
    reader->longs_size = longs_size;
    reader->doc_count = doc_count;
    reader->sum_total_term_freq = sum_total_term_freq;
    reader->parent = parent;
    // todo other fields if needed

    if (index_in != NULL) {
        mram_reader_t *clone = mram_reader_clone(index_in);
        set_index(clone, (uint32_t) index_start_fp);
        reader->index = fst_new(clone);
    } else {
        reader->index = NULL;
    }

    return reader;
}