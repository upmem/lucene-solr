/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include <stdlib.h>
#include "data_input.h"
#include "allocation.h"

data_input_t* data_input_clone(data_input_t* from) {
    data_input_t* result = allocation_get(sizeof(*result));
    memcpy(result, from, sizeof(*from));
    return result;
}

void read_bytes(data_input_t* input, uint8_t* dest, uint32_t offset, uint32_t length) {
    for (uint32_t i = 0; i < length; ++i) {
        dest[offset + i] = input->read_byte(input);
    }
}

uint16_t read_short(data_input_t* input) {
    return (uint16_t) (((input->read_byte(input) & 0xFF) << 8) | (input->read_byte(input) & 0xFF));
}

uint32_t read_int(data_input_t* input) {
    return (uint32_t) (((input->read_byte(input) & 0xFF) << 24) | ((input->read_byte(input) & 0xFF) << 16)|
        ((input->read_byte(input) & 0xFF) <<  8) | (input->read_byte(input) & 0xFF));
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

uint64_t read_long(data_input_t* input) {
    return (uint64_t) (((  (uint64_t)(input->read_byte(input) & 0xFF)) << 56)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 48)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 40)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 32)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 24)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 16)
                       | (((uint64_t)(input->read_byte(input) & 0xFF)) << 8)
                       | (((uint64_t) input->read_byte(input) & 0xFF)));
}

uint64_t read_vlong(data_input_t* input) {
    uint8_t b = input->read_byte(input);
    uint64_t i = (uint64_t) (b & 0x7F);
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = input->read_byte(input);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

char* read_string(data_input_t* input, uint32_t* length) {
    *length = read_vint(input);
    char* string = allocation_get(*length + 1);
    read_bytes(input, (uint8_t *) string, 0, *length);
    string[*length] = '\0';
    return string;
}

string_map_t* read_map_of_strings(data_input_t* input) {
    string_map_t* result = allocation_get(sizeof(*result));
    result->nr_entries = read_vint(input);
    result->entries = allocation_get(result->nr_entries * sizeof(*(result->entries)));
    for (int i = 0; i < result->nr_entries; ++i) {
        string_map_entry_t* entry = result->entries + i;
        uint32_t unused_length;
        entry->key = read_string(input, &unused_length);
        entry->value = read_string(input, &unused_length);
    }

    return result;
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