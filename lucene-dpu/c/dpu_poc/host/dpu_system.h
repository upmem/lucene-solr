/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_DPU_SYSTEM_H
#define DPU_POC_DPU_SYSTEM_H

#include <stdbool.h>
#include <dpu.h>
#include "segment_files.h"

typedef struct {
    unsigned int nr_ranks;
    unsigned int nr_dpus;
    struct dpu_rank_t **ranks;
} dpu_system_t;

dpu_system_t *initialize_dpu_system(unsigned int nr_dpus, const char *dpu_binary, dpu_type_t dpu_type, char* dpu_profile);
void free_dpu_system(dpu_system_t *dpu_system);
bool prepare_mram_with_segments(dpu_system_t *dpu_system, unsigned int dpu_idx, mram_image_t *mram_image);
bool search(dpu_system_t *dpu_system,
            const char *field,
            const char *value,
            bool save_memory_image,
            unsigned int nb_fields,
            char **field_names);

#endif //DPU_POC_DPU_SYSTEM_H
