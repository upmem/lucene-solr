/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_DPU_SYSTEM_H
#define DPU_POC_DPU_SYSTEM_H

#include <stdbool.h>
#include <dpu.h>
#include "segment_files.h"

typedef struct {
    dpu_rank_t rank;
    dpu_t dpu;
} dpu_system_t;

dpu_system_t *initialize_dpu_system(const char *dpu_binary, dpu_type_t dpu_type, char* dpu_profile);
void free_dpu_system(dpu_system_t *dpu_system);
bool prepare_mrams_with_segments(dpu_system_t *dpu_system, mram_image_t *mram_image);
bool search(dpu_system_t *dpu_system, const char *field, const char *value, bool save_memory_image);

#endif //DPU_POC_DPU_SYSTEM_H
