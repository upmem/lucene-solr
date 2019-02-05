/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stddef.h>
#include "segments_info.h"
#include "alloc_wrapper.h"
#include "codec_footer.h"

segments_info_t *parse_segments_info(mram_addr_t offset) {
    segments_info_t *segments_info;
    mram_reader_t buffer = {
            .index = offset,
            .base = offset,
    };
    unsigned int each;
    uint32_t unused_length;

    segments_info = malloc(sizeof(segments_info_t));

    segments_info->index_header = read_index_header(&buffer);

    segments_info->SegVersion[0] = mram_read_int(&buffer, false);
    segments_info->SegVersion[1] = mram_read_int(&buffer, false);
    segments_info->SegVersion[2] = mram_read_int(&buffer, false);

    segments_info->HasMinVersion = mram_read_byte(&buffer, false);

    if (segments_info->HasMinVersion) {
        segments_info->MinVersion[0] = mram_read_int(&buffer, false);
        segments_info->MinVersion[1] = mram_read_int(&buffer, false);
        segments_info->MinVersion[2] = mram_read_int(&buffer, false);
    }

    segments_info->DocCount = mram_read_int(&buffer, false);

    segments_info->IsCompoundFile = mram_read_byte(&buffer, false);

    segments_info->DiagnosticsCount = mram_read_vint(&buffer, false);
    segments_info->Diagnostics = (uint8_t ***) malloc(segments_info->DiagnosticsCount * sizeof(uint8_t **));
    for (each = 0; each < segments_info->DiagnosticsCount; each++) {
        segments_info->Diagnostics[each] = (uint8_t **) malloc(2 * sizeof(uint8_t *));

        segments_info->Diagnostics[each][0] = mram_read_string(&buffer, &unused_length, false);
        segments_info->Diagnostics[each][1] = mram_read_string(&buffer, &unused_length, false);
    }

    segments_info->FilesCount = mram_read_vint(&buffer, false);
    segments_info->Files = (uint8_t **) malloc(segments_info->FilesCount * sizeof(uint8_t *));
    for (each = 0; each < segments_info->FilesCount; each++) {
        segments_info->Files[each] = mram_read_string(&buffer, &unused_length, false);
    }

    segments_info->AttributesCount = mram_read_vint(&buffer, false);
    segments_info->Attributes = (uint8_t ***) malloc(segments_info->AttributesCount * sizeof(uint8_t **));
    for (each = 0; each < segments_info->AttributesCount; each++) {
        segments_info->Attributes[each] = (uint8_t **) malloc(2 * sizeof(uint8_t *));

        segments_info->Attributes[each][0] = mram_read_string(&buffer, &unused_length, false);
        segments_info->Attributes[each][1] = mram_read_string(&buffer, &unused_length, false);
    }

    segments_info->NumSortFields = mram_read_vint(&buffer, false);

    segments_info->codec_footer = read_codec_footer(&buffer);

    return segments_info;
}