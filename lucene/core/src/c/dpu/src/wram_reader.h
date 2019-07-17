/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_WRAM_READER_H
#define DPU_POC_WRAM_READER_H

#include <stdint.h>

typedef struct {
    uint8_t *buffer;
    uint32_t index;
} wram_reader_t;

uint8_t wram_read_byte(wram_reader_t *reader);
void wram_read_bytes(wram_reader_t *reader, uint8_t *dest, uint32_t offset, uint32_t length);
void wram_skip_bytes(wram_reader_t *reader, uint32_t length);
uint32_t wram_read_vint(wram_reader_t *reader);
uint64_t wram_read_vlong(wram_reader_t *reader);
uint32_t wram_read_false_vlong(wram_reader_t *reader);

#endif //DPU_POC_WRAM_READER_H
