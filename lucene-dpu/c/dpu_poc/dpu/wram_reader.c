/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include "wram_reader.h"

uint8_t wram_read_byte(wram_reader_t *reader) {
    return reader->buffer[reader->index++];
}

void wram_read_bytes(wram_reader_t *reader, uint8_t *dest, uint32_t offset, uint32_t length) {
    memcpy(dest + offset, reader->buffer + reader->index, length);
    reader->index += length;
}

void wram_skip_bytes(wram_reader_t *reader, uint32_t length) {
    reader->index += length;
}

uint32_t wram_read_vint(wram_reader_t *reader) {
    uint8_t b = wram_read_byte(reader);
    uint32_t i = (uint32_t) (b & 0x7F);
    // todo we could inline this loop
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = wram_read_byte(reader);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

uint64_t wram_read_vlong(wram_reader_t *reader) {
    uint8_t b = wram_read_byte(reader);
    uint64_t i = (uint64_t) (b & 0x7F);
    // todo we could inline this loop
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = wram_read_byte(reader);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

uint32_t wram_read_false_vlong(wram_reader_t *reader) {
    return wram_read_vint(reader);
}