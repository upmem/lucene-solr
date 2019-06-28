/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include <query.h>
#include <string.h>
#include <mram_structure.h>
#include <dpu_log.h>
#include "dpu_system.h"
#include "query.h"
#include "x86_process/term_scorer.h"

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value, unsigned int *, char ***);
static bool process_results(dpu_system_t *dpu_system);
static bool save_mram(dpu_system_t *dpu_system, const char* path);

dpu_system_t* initialize_dpu_system(const char* dpu_binary, dpu_type_t dpu_type, char* dpu_profile) {
    dpu_system_t *dpu_system = malloc(sizeof(*dpu_system));

    if (dpu_system == NULL) {
        return NULL;
    }

    struct dpu_logging_config_t log = {
            .source = KTRACE,
            .destination_directory_name = "/tmp"
    };

    struct dpu_param_t params = {
            .type = dpu_type,
            .profile = dpu_profile,
            .logging_config = &log
    };

    if (dpu_alloc(&params, &dpu_system->rank) != DPU_API_SUCCESS) {
        fprintf(stderr, "cannot allocate rank\n");
        free(dpu_system);
        return NULL;
    }

    dpu_system->dpu = dpu_get(dpu_system->rank, 0, 0);

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
    if (dpu_copy_to_dpu(dpu_system->dpu, mram_image->content, 0, mram_image->current_offset) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading a mram image\n");
        return false;
    }

    return true;
}

bool search(dpu_system_t *dpu_system,
            const char *field,
            const char *value,
            bool save_memory_image,
            unsigned int *nb_fields_per_thread,
            char ***field_names_per_thread) {
    if (!load_query(dpu_system, field, value, nb_fields_per_thread, field_names_per_thread)) {
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

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value, unsigned int *nb_field, char ***field_names) {
    size_t value_length = strlen(value);

    if (value_length >= MAX_VALUE_SIZE) {
        fprintf(stderr, "specified query value is too big\n");
        return false;
    }
    query_t query;
    memcpy(&query.value, value, value_length);
    query.value[value_length] = '\0';

    unsigned int each_field;
    query.field_id = -1;
    for (each_field = 0; each_field < nb_field[0]; each_field++) {
        char *field_name = field_names[0][each_field];
        if (field_name != NULL && strcmp(field, field_name) == 0) {
            unsigned each_thread;

            /* Check to make sure that every segment have the same field name at the same id */
            for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
                char *field_name = field_names[each_thread][each_field];
                if (field_name == NULL || strcmp(field, field_name) != 0) {
                    fprintf(stderr, "segment does not have the same field name for a field id\n");
                    return false;
                }
            }

            query.field_id = each_field;
            break;
        }
    }
    if (query.field_id == -1) {
        fprintf(stderr, "error when looking for field id\n");
        return false;
    }
    if (dpu_copy_to_dpu(dpu_system->dpu, (const uint8_t *)&query, QUERY_BUFFER_OFFSET, sizeof(query)) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading the query field\n");
        return false;
    }

    return true;
}

static bool process_results(dpu_system_t *dpu_system) {
    dpu_output_t *results = (dpu_output_t *)malloc(OUTPUTS_BUFFER_SIZE);
    if (dpu_copy_from_dpu(dpu_system->dpu,
                          OUTPUTS_BUFFER_OFFSET,
                          (uint8_t *)results,
                          OUTPUTS_BUFFER_SIZE) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when reading the results\n");
        free(results);
        return false;
    }
    dpu_idf_output_t idf_output;
    if (dpu_copy_from_dpu(dpu_system->dpu, IDF_OUTPUT_OFFSET, (uint8_t *)&idf_output, IDF_OUTPUT_SIZE) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when reading the idf_output\n");
        free(results);
        return false;
    }

    unsigned int each_thread;
    printf("######## RESULTS #########\n");
    for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
        dpu_output_t *curr_result = &results[each_thread * OUTPUTS_PER_THREAD];
        while (curr_result->doc_id != 0xffffffff) {
            printf("[%u] doc:%u \tscore:%f\n",
                   each_thread,
                   curr_result->doc_id,
                   compute_score(idf_output.doc_count,
                                 idf_output.doc_freq,
                                 curr_result->freq,
                                 curr_result->doc_norm,
                                 idf_output.total_term_freq));
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
    if (dpu_copy_from_dpu(dpu_system->dpu, 0, mram, MRAM_SIZE) != DPU_API_SUCCESS) {
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
