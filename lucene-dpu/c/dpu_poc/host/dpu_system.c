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

bool prepare_mrams_with_segments(dpu_system_t *dpu_system, mram_image_t *mram_image) {
    if (dpu_copy_to_individual(dpu_system->dpu, mram_image->content, 0, mram_image->current_offset) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading a mram image\n");
        return false;
    }

    return true;
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
        dpu_gather_postmortem_information(dpu_system->dpu, stdout);
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

#define NR_TOTAL_OUTPUT (NR_THREADS * OUTPUTS_PER_THREAD)
static bool process_results(dpu_system_t *dpu_system) {
    dpu_output_t *results = (dpu_output_t *)malloc(NR_TOTAL_OUTPUT * sizeof(dpu_output_t));
    if (dpu_copy_from_individual(dpu_system->dpu, 0, (uint8_t *)results, sizeof(dpu_output_t) * NR_TOTAL_OUTPUT) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when reading the results\n");
        free(results);
        return false;
    }
    unsigned int each_thread;
    for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
        dpu_output_t *curr_result = &results[each_thread * OUTPUTS_PER_THREAD];
        while (curr_result->doc_id != 0xffffffff) {
            printf("[%u] doc:%u \tscore:%f\n",
                   each_thread,
                   curr_result->doc_id,
                   ((float)curr_result->score) / ((float)SCORE_PRECISION));
            curr_result++;
        }
    }
    free(results);
    return true;
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