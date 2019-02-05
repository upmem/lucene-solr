/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "codec_footer.h"
#include "alloc_wrapper.h"

codec_footer_t *read_codec_footer(mram_reader_t *buffer) {
    codec_footer_t *codec_footer = malloc(sizeof(codec_footer_t));

    codec_footer->magic_footer = mram_read_int(buffer, false);
    codec_footer->algorithm_id = mram_read_int(buffer, false);
    codec_footer->checksum = mram_read_long(buffer, false);

    return codec_footer;
}
