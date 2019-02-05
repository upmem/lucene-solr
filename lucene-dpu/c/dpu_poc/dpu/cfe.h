/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_CFE_H
#define DPU_POC_CFE_H

#include <mram.h>
#include "index_header.h"
#include "codec_footer.h"

typedef struct {
    uint8_t *entry_name;
    uint64_t offset;
    uint64_t length;
} cfe_entries_t;

typedef struct {
    index_header_t *index_header;
    int32_t num_entries;
    cfe_entries_t *entries;
    codec_footer_t *codec_footer;
} cfe_info_t;

cfe_info_t *parse_cfe_info(mram_addr_t offset);

#endif //DPU_POC_CFE_H
