/*
 * Copyright (c) 2014-2018 - uPmem
 */

#ifndef X86_POC_UTILS_H
#define X86_POC_UTILS_H

#include <stdint.h>

typedef struct _data_input_t {
    uint8_t* buffer;
    uint32_t index;

    uint8_t (*read_byte)(struct _data_input_t* self);
    void (*skip_bytes)(struct _data_input_t* self, uint32_t length);
} data_input_t;

void read_bytes(struct _data_input_t* self, uint8_t* dest, uint32_t length);
uint32_t read_vint(data_input_t* input);
char* read_string(data_input_t* input, uint32_t* length);

uint8_t incremental_read_byte(data_input_t* input);
void incremental_skip_bytes(data_input_t* input, uint32_t length);
uint8_t decremental_read_byte(data_input_t* input);
void decremental_skip_bytes(data_input_t* input, uint32_t length);

#endif //X86_POC_UTILS_H
