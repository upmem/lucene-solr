/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include "mram_reader.h"
#include "alloc_wrapper.h"

void mram_reader_fill(mram_reader_t *dest, mram_reader_t *from) {
    dest->index = from->index;
    dest->base = from->base;
    dest->cache = from->cache;
}

void set_index(mram_reader_t *input, uint32_t index) {
    input->index = input->base + index;
}

uint8_t mram_read_byte(mram_reader_t *reader, bool decrement) {
    uint32_t offset = update_mram_cache(reader->cache, reader->index);

    if (decrement) {
        reader->index--;
    } else {
        reader->index++;
    }

    return reader->cache->contents[offset];
}

void mram_skip_bytes(mram_reader_t *reader, uint32_t length, bool decrement) {
    if (decrement) {
        reader->index -= length;
    } else {
        reader->index += length;
    }
}

void mram_read_bytes(mram_reader_t *reader, uint8_t* dest, uint32_t offset, uint32_t length, bool decrement) {
    // todo something more efficient?
    for (int i = 0; i < length; ++i) {
        dest[offset + i] = mram_read_byte(reader, decrement);
    }
}

uint16_t mram_read_short(mram_reader_t *reader, bool decrement) {
    // todo something more efficient?
    return (uint16_t) (((mram_read_byte(reader, decrement) & 0xFF) << 8) | (mram_read_byte(reader, decrement) & 0xFF));
}

uint32_t mram_read_int(mram_reader_t *reader, bool decrement) {
    // todo something more efficient?
    return ((((uint32_t) mram_read_short(reader, decrement)) & 0xffff) << 16) | (((uint32_t) mram_read_short(reader, decrement)) & 0xffff);
}

uint32_t mram_read_vint(mram_reader_t *reader, bool decrement) {
    // todo something more efficient?
    uint8_t b = mram_read_byte(reader, decrement);
    uint32_t i = (uint32_t) (b & 0x7F);
    // todo we could inline this loop
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = mram_read_byte(reader, decrement);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

uint64_t mram_read_long(mram_reader_t *reader, bool decrement) {
    // todo something more efficient?
    return ((((uint64_t) mram_read_int(reader, decrement)) & 0xffffffffl) << 32) |
            (((uint64_t) mram_read_int(reader, decrement)) & 0xffffffffl);
}

uint64_t mram_read_vlong(mram_reader_t *reader, bool decrement) {
    // todo something more efficient?
    uint8_t b = mram_read_byte(reader, decrement);
    uint64_t i = (uint64_t) (b & 0x7F);
    // todo we could inline this loop
    for (uint32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = mram_read_byte(reader, decrement);
        i |= (b & 0x7F) << shift;
    }
    return i;
}

uint32_t mram_read_false_vlong(mram_reader_t *reader, bool decrement) {
    return mram_read_vint(reader, decrement);
}

void mram_read_string(mram_reader_t *reader, char* string, bool decrement) {
    // todo something more efficient?
    uint32_t length = mram_read_vint(reader, decrement);
    mram_read_bytes(reader, (uint8_t *) string, 0, length, decrement);
    string[length] = '\0';
}

void mram_read_string_dummy(mram_reader_t *reader, bool decrement) {
    uint32_t length = mram_read_vint(reader, decrement);
    mram_skip_bytes(reader, length, decrement);
}

void mram_read_map_of_strings_dummy(mram_reader_t *reader, bool decrement) {
    uint32_t nr_entries = mram_read_vint(reader, decrement);
    for (int i = 0; i < nr_entries; ++i) {
        mram_read_string_dummy(reader, decrement);
        mram_read_string_dummy(reader, decrement);
    }
}
