/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "index_header.h"
#include "alloc_wrapper.h"

void read_index_header(index_header_t *index_header, mram_reader_t *buffer) {
    index_header->magic = mram_read_int(buffer, false);
    // codecName here
    mram_read_string_dummy(buffer, false);
    index_header->version = mram_read_int(buffer, false);

    mram_read_bytes(buffer, (uint8_t *)index_header->object_id, 0, LUCENE_INDEX_HEADER_OBJECTID_SIZE, false);

    index_header->suffix_length = mram_read_byte(buffer, false);
    mram_skip_bytes(buffer, index_header->suffix_length, false);
}

uint32_t check_index_header(mram_reader_t *in) {
    // todo this function does not check anything, just read the expected bytes and returns the version
    index_header_t header;
    read_index_header(&header, in);
    uint32_t version = header.version;
    return version;
}

uint32_t check_header(mram_reader_t *in) {
    // todo this function does not check anything, just read the expected bytes and returns the version
    // actualHeader here
    mram_read_int(in, false);
    // actualCode here
    mram_read_string_dummy(in, false);
    uint32_t actual_version = mram_read_int(in, false);
    return actual_version;
}