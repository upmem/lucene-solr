/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "index_header.h"
#include "alloc_wrapper.h"

index_header_t *read_index_header(mram_reader_t *buffer) {
    uint32_t unused_length;
    index_header_t *index_header = malloc(sizeof(index_header_t));

    index_header->magic = mram_read_int(buffer, false);
    index_header->codec_name = (uint8_t *)mram_read_string(buffer, &unused_length, false);
    index_header->version = mram_read_int(buffer, false);

    mram_read_bytes(buffer, (uint8_t *)index_header->object_id, 0, LUCENE_INDEX_HEADER_OBJECTID_SIZE, false);

    index_header->suffix_length = mram_read_byte(buffer, false);
    index_header->suffix_bytes = malloc(index_header->suffix_length * sizeof(uint8_t));
    mram_read_bytes(buffer, index_header->suffix_bytes, 0, index_header->suffix_length, false);

    return index_header;
}

uint32_t check_index_header(mram_reader_t *in) {
    // todo this function does not check anything, just read the expected bytes and returns the version
    index_header_t *header = read_index_header(in);
    uint32_t version = header->version;
    return version;
}

uint32_t check_header(mram_reader_t *in) {
    // todo this function does not check anything, just read the expected bytes and returns the version
    uint32_t actual_header = mram_read_int(in, false);
    uint32_t actual_codec_length;
    char *actual_code = mram_read_string(in, &actual_codec_length, false);
    uint32_t actual_version = mram_read_int(in, false);
    return actual_version;
}