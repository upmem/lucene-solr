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

static block_tree_term_reader_t* build_fields_producer(file_buffer_t* file_buffers);
static void search(block_tree_term_reader_t* reader, char* field, char* value);

int main(int argc, char **argv)
{
    file_buffer_t* file_buffers;

    file_buffers = get_file_buffers(argv[1], 0);

    block_tree_term_reader_t* reader = build_fields_producer(file_buffers);

    search(reader, "contents", "apache");
    search(reader, "contents", "patent");
    search(reader, "contents", "lucene");
    search(reader, "contents", "gnu");
    search(reader, "contents", "derivative");
    search(reader, "contents", "license");

    free_file_buffers(file_buffers);

    return 0;
}

static block_tree_term_reader_t* build_fields_producer(file_buffer_t* file_buffers) {
    file_buffer_t* field_infos_buffer = file_buffers + LUCENE_FILE_FNM;

    field_infos_t* field_infos = read_field_infos(field_infos_buffer);

    file_buffer_t* terms_dict = file_buffers + LUCENE_FILE_TIM;
    file_buffer_t* terms_index = file_buffers + LUCENE_FILE_TIP;

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

static void search(block_tree_term_reader_t* reader, char* field, char* value) {
    term_t* term = term_from_string(field, value);

    field_reader_t* terms = get_terms(reader, term->field);
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
}