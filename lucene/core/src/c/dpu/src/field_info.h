/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FIELD_INFO_H
#define DPU_POC_FIELD_INFO_H

#include "file.h"
#include "string_map.h"
#include <query.h>
#include <stdbool.h>
#include <stdint.h>

typedef enum {
    DOC_VALUES_TYPE_NONE,
    DOC_VALUES_TYPE_NUMERIC,
    DOC_VALUES_TYPE_BINARY,
    DOC_VALUES_TYPE_SORTED,
    DOC_VALUES_TYPE_SORTED_NUMERIC,
    DOC_VALUES_TYPE_SORTED_SET
} doc_values_type_t;

typedef enum {
    INDEX_OPTIONS_NONE,
    INDEX_OPTIONS_DOCS,
    INDEX_OPTIONS_DOCS_AND_FREQS,
    INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS,
    INDEX_OPTIONS_DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
} index_options_t;

static inline int32_t compare_index_options(index_options_t first, index_options_t second)
{
    return ((int32_t)first) - ((int32_t)second);
}

#endif // DPU_POC_FIELD_INFO_H
