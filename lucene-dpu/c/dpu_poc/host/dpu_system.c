/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include <query.h>
#include <string.h>
#include <mram_structure.h>
#include <dpulog.h>
#include "dpu_system.h"

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value);
static bool load_file_at(dpu_t dpu, mram_addr_t offset, segment_file_t *file);
static bool load_file_indices_at(dpu_t dpu, mram_addr_t offset, mram_addr_t si_addr, mram_addr_t cfe_addr, mram_addr_t cfs_addr);
static bool process_results(dpu_system_t *dpu_system);
static bool save_mram(dpu_system_t *dpu_system, const char* path);

dpu_system_t* initialize_dpu_system(const char* dpu_binary, dpu_type_t dpu_type, char* dpu_profile) {
    dpu_system_t *dpu_system = malloc(sizeof(*dpu_system));

    if (dpu_system == NULL) {
        return NULL;
    }

    dpu_logging_config_t log = {
            .source = KTRACE,
            .destination_directory_name = "."
    };

    dpu_param_t params = {
            .type = dpu_type,
            .profile = dpu_profile,
            .logging_config = &log
    };

    if (dpu_alloc(&params, &dpu_system->rank) != DPU_API_SUCCESS) {
        fprintf(stderr, "cannot allocate rank\n");
        free(dpu_system);
        return NULL;
    }

    dpu_system->dpu = dpu_get_id(dpu_system->rank, 0);

    if (dpu_load_individual(dpu_system->dpu, dpu_binary) != DPU_API_SUCCESS) {
        fprintf(stderr, "cannot load program in dpu\n");
        dpu_free(dpu_system->rank);
        free(dpu_system);
        return NULL;
    }

    return dpu_system;
}

void free_dpu_system(dpu_system_t *dpu_system) {
    dpu_free(dpu_system->rank);
    free(dpu_system);
}

bool prepare_mrams_with_segments(dpu_system_t *dpu_system, segment_files_t *segment_files) {
    mram_addr_t si_file_offset = FILES_SUMMARY_BUFFER_OFFSET + FILES_SUMMARY_BUFFER_SIZE;
    mram_addr_t cfe_file_offset = DMA_ALIGNED(si_file_offset + segment_files->si_file.length);
    mram_addr_t cfs_file_offset = DMA_ALIGNED(cfe_file_offset + segment_files->cfe_file.length);

    return load_file_at(dpu_system->dpu, si_file_offset, &segment_files->si_file) &&
            load_file_at(dpu_system->dpu, cfe_file_offset, &segment_files->cfe_file) &&
            load_file_at(dpu_system->dpu, cfs_file_offset, &segment_files->cfs_file) &&
            load_file_indices_at(dpu_system->dpu, FILES_SUMMARY_BUFFER_OFFSET, si_file_offset, cfe_file_offset, cfs_file_offset);
}

bool search(dpu_system_t *dpu_system, const char *field, const char *value, bool save_memory_image) {
    if (!load_query(dpu_system, field, value)) {
        fprintf(stderr, "error when loading the query\n");
        return false;
    }

    if (save_memory_image && !save_mram(dpu_system, "input.mram.image")) {
        fprintf(stderr, "error when saving the memory image\n");
        return false;
    }

    if (dpu_boot_individual(dpu_system->dpu, SYNCHRONOUS) != DPU_API_SUCCESS) {
        fprintf(stderr, "error during DPU execution\n");
//        dpu_gather_postmortem_information(dpu_system->dpu, stdout);
        dpulog_read_for_dpu(dpu_system->dpu, stdout);
        return false;
    }

    if (!dpulog_read_for_dpu(dpu_system->dpu, stdout)) {
        fprintf(stderr, "could not read DPU log\n");
    }

    if (!process_results(dpu_system)) {
        fprintf(stderr, "error when fetching results\n");
        return false;
    }

    return true;
}

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value) {
    size_t field_length = strlen(field);
    size_t value_length = strlen(value);

    if (field_length >= MAX_FIELD_SIZE) {
        fprintf(stderr, "specified query field is too big\n");
        return false;
    }
    if (value_length >= MAX_VALUE_SIZE) {
        fprintf(stderr, "specified query value is too big\n");
        return false;
    }

    if (dpu_copy_to_individual(dpu_system->dpu, field, QUERY_BUFFER_OFFSET, field_length + 1) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading the query field\n");
        return false;
    }
    if (dpu_copy_to_individual(dpu_system->dpu, value, QUERY_BUFFER_OFFSET + MAX_FIELD_SIZE, value_length + 1) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading the query value\n");
        return false;
    }

    return true;
}

static bool load_file_at(dpu_t dpu, mram_addr_t offset, segment_file_t* file) {
    if (dpu_copy_to_individual(dpu, file->content, offset, file->length) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading a segment file\n");
        return false;
    }

    return true;
}

static bool load_file_indices_at(dpu_t dpu, mram_addr_t offset, mram_addr_t si_addr, mram_addr_t cfe_addr, mram_addr_t cfs_addr) {
    mram_addr_t addresses[3] = { si_addr, cfe_addr, cfs_addr };

    if (dpu_copy_to_individual(dpu, (uint8_t*) addresses, offset, sizeof(addresses)) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading segment file indices\n");
        return false;
    }

    return true;
}

static bool process_results(dpu_system_t *dpu_system) {
    // todo
    return false;
}

static bool save_mram(dpu_system_t *dpu_system, const char* path) {
    FILE* file = fopen(path, "wb");

    if (file == NULL) {
        fprintf(stderr, "cannot open file %s\n", path);
        return false;
    }

    uint8_t *mram = malloc(MRAM_SIZE);
    if (dpu_copy_from_individual(dpu_system->dpu, 0, mram, MRAM_SIZE) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when fetching the mram image\n");
        free(mram);
        fclose(file);
        return false;
    }

    fwrite(mram, 1, MRAM_SIZE, file);

    free(mram);
    fclose(file);
    return true;
}