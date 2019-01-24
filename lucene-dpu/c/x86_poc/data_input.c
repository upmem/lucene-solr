/*
 * Copyright (c) 2014-2018 - uPmem
 */

#include <string.h>
#include <stdlib.h>
#include "data_input.h"

void read_bytes(data_input_t* input, uint8_t* dest, uint32_t length) {
    for (uint32_t i = 0; i < length; ++i) {
        dest[i] = input->read_byte(input);
    }
}

uint32_t read_vint(data_input_t* input) {
    uint8_t b = input->read_byte(input);
    uint32_t i = (uint32_t) (b & 0x7F);
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = input->read_byte(input);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

char* read_string(data_input_t* input, uint32_t* length) {
    *length = read_vint(input);
    char* string = malloc(*length);
    read_bytes(input, (uint8_t *) string, *length);
    return string;
}

uint8_t incremental_read_byte(data_input_t* input) {
    uint8_t result = input->buffer[input->index];
    input->index++;
    return result;
}

void incremental_skip_bytes(data_input_t* input, uint32_t length) {
    input->index += length;
}

uint8_t decremental_read_byte(data_input_t* input) {
    uint8_t result = input->buffer[input->index];
    input->index--;
    return result;
}

void decremental_skip_bytes(data_input_t* input, uint32_t length) {
    input->index -= length;
}