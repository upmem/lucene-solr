/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_SEGMENT_FILES_H
#define DPU_POC_SEGMENT_FILES_H

#include <stdint.h>

typedef struct {
    uint32_t current_offset;
    uint32_t nr_segments;
    uint8_t *content;
} mram_image_t;

mram_image_t *mram_image_new();
void mram_image_reset(mram_image_t *mram_image);
uint32_t discover_segments(const char* index_directory, char ***segment_suffixes);
bool load_segment_files(mram_image_t *mram_image,
                        const char* index_directory,
                        uint32_t segment_number,
                        const char* segment_suffix,
                        unsigned int *nb_field,
                        char ***field_names);
void load_empty_segment(mram_image_t *mram_image, uint32_t segment_number);
void free_mram_image(mram_image_t *mram_image);

#endif //DPU_POC_SEGMENT_FILES_H
