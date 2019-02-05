/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_QUERY_H
#define DPU_POC_QUERY_H

#define MAX_FIELD_SIZE 8
#define MAX_VALUE_SIZE 8

typedef struct {
    char field[MAX_FIELD_SIZE];
    char value[MAX_VALUE_SIZE];
} query_t;

#endif //DPU_POC_QUERY_H
