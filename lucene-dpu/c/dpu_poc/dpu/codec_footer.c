/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "codec_footer.h"
#include "alloc_wrapper.h"

void read_codec_footer(codec_footer_t *codec_footer, mram_reader_t *buffer) {
    codec_footer->magic_footer = mram_read_int(buffer, false);
    codec_footer->algorithm_id = mram_read_int(buffer, false);
    codec_footer->checksum = mram_read_long(buffer, false);
}
