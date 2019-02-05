/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_CODEC_FOOTER_H
#define DPU_POC_CODEC_FOOTER_H

#include "mram_reader.h"

typedef struct {
    int32_t magic_footer;
    int32_t algorithm_id;
    uint64_t checksum;
} codec_footer_t;

codec_footer_t *read_codec_footer(mram_reader_t *buffer);

#endif //DPU_POC_CODEC_FOOTER_H
