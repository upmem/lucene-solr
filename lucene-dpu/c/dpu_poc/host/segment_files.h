/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_SEGMENT_FILES_H
#define DPU_POC_SEGMENT_FILES_H

#include <stdint.h>

typedef struct {
    uint32_t length;
    uint8_t* content;
} segment_file_t;

typedef struct {
    segment_file_t si_file;
    segment_file_t cfe_file;
    segment_file_t cfs_file;
} segment_files_t;

segment_files_t* load_segment_files(const char* index_directory, uint32_t segment_number);
void free_segment_files(segment_files_t* segment_files);

#endif //DPU_POC_SEGMENT_FILES_H
