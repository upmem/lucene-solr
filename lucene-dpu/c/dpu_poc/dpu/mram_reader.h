/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_MRAM_READER_H
#define DPU_POC_MRAM_READER_H

#include <stdint.h>
#include <mram.h>
#include <stdbool.h>
#include "string_map.h"
#include "mram_cache.h"

typedef struct _mram_reader_t mram_reader_t;

struct _mram_reader_t {
    mram_addr_t index;
    mram_addr_t base;
    mram_cache_t* cache;
};

mram_reader_t *mram_reader_clone(mram_reader_t* reader);
void set_index(mram_reader_t *input, uint32_t index);

uint8_t mram_read_byte(mram_reader_t *reader, bool decrement);
void mram_read_bytes(mram_reader_t *reader, uint8_t* dest, uint32_t offset, uint32_t length, bool decrement);
uint16_t mram_read_short(mram_reader_t *reader, bool decrement);
uint32_t mram_read_int(mram_reader_t *reader, bool decrement);
uint32_t mram_read_vint(mram_reader_t *reader, bool decrement);
uint64_t mram_read_long(mram_reader_t *reader, bool decrement);
uint64_t mram_read_vlong(mram_reader_t *reader, bool decrement);
void mram_skip_bytes(mram_reader_t *reader, uint32_t length, bool decrement);

char *mram_read_string(mram_reader_t *reader, uint32_t *length, bool decrement);
string_map_t *mram_read_map_of_strings(mram_reader_t *reader, bool decrement);

#endif //DPU_POC_MRAM_READER_H
