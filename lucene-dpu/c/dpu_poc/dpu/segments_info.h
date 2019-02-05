/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_SEGMENTS_INFO_H
#define DPU_POC_SEGMENTS_INFO_H

#include <mram.h>
#include "index_header.h"
#include "codec_footer.h"

typedef struct {
    index_header_t *index_header;
    int32_t SegVersion[3];
    uint8_t HasMinVersion;
    int32_t MinVersion[3];
    int32_t DocCount;
    uint8_t IsCompoundFile;
    int32_t DiagnosticsCount;
    uint8_t ***Diagnostics;
    int32_t FilesCount;
    uint8_t **Files;
    int32_t AttributesCount;
    uint8_t ***Attributes;
    int32_t NumSortFields;
    codec_footer_t *codec_footer;
} segments_info_t;

segments_info_t *parse_segments_info(mram_addr_t offset);

#endif //DPU_POC_SEGMENTS_INFO_H
