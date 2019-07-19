/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_QUERY_H
#define DPU_POC_QUERY_H

#include <stdint.h>

#define MAX_FIELD_SIZE 16
#define MAX_VALUE_SIZE 20

typedef struct {
    char value[MAX_VALUE_SIZE];
    uint32_t field_id;
} query_t;

#endif // DPU_POC_QUERY_H
