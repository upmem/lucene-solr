/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include <stdio.h>
#include "parser.h"
#include "segment_terms_enum.h"
#include "field_infos.h"
#include "term.h"

int main(int argc, char **argv)
{
    file_buffer_t* file_buffers;

    file_buffers = get_file_buffers(argv[1], 0);

    file_buffer_t* field_infos_buffer = file_buffers + LUCENE_FILE_FNM;

    field_infos_t* field_infos = read_field_infos(field_infos_buffer);

    file_buffer_t* terms_dict = file_buffers + LUCENE_FILE_TIM;
    file_buffer_t* terms_index = file_buffers + LUCENE_FILE_TIP;

    data_input_t terms_in = {
            .index = 0,
            .buffer = terms_dict->content,
            .read_byte = incremental_read_byte,
            .skip_bytes = incremental_skip_bytes
    };

    data_input_t index_in = {
            .index = 0,
            .buffer = terms_index->content,
            .read_byte = incremental_read_byte,
            .skip_bytes = incremental_skip_bytes
    };

    block_tree_term_reader_t* reader = block_tree_term_reader_new(field_infos, &terms_in, (uint32_t) terms_dict->length,
            &index_in, (uint32_t) terms_index->length);

    term_t* term = term_from_string("contents", "apache");

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

    free_file_buffers(file_buffers);

    return 0;
}
