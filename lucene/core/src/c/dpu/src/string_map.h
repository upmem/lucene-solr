/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_STRING_MAP_H
#define DPU_POC_STRING_MAP_H

#include <stdint.h>

typedef struct {
    char *key;
    char *value;
} string_map_entry_t;

typedef struct {
    uint32_t nr_entries;
    string_map_entry_t *entries;
} string_map_t;

#endif //DPU_POC_STRING_MAP_H
