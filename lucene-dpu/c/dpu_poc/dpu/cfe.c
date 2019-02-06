/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "cfe.h"
#include "alloc_wrapper.h"

cfe_info_t *parse_cfe_info(mram_addr_t offset) {
    cfe_info_t *cfe_info;
    mram_reader_t buffer = {
            .index = offset,
            .base = offset,
    };
    unsigned int each;
    uint32_t unused_length;

    cfe_info = malloc(sizeof(cfe_info_t));

    cfe_info->index_header = read_index_header(&buffer);

    cfe_info->num_entries = mram_read_vint(&buffer, false);

    cfe_info->entries = malloc(sizeof(cfe_entries_t) * cfe_info->num_entries);

    for (each = 0; each < cfe_info->num_entries; each++) {
        cfe_info->entries[each].entry_name = (uint8_t *)mram_read_string(&buffer, &unused_length, false);
        cfe_info->entries[each].offset = mram_read_long(&buffer, false);
        cfe_info->entries[each].length = mram_read_long(&buffer, false);
    }

    cfe_info->codec_footer = read_codec_footer(&buffer);

    return cfe_info;
}