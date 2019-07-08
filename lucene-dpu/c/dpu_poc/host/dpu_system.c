/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include <query.h>
#include <string.h>
#include <mram_structure.h>
#include <dpu_log.h>
#include <dpu_description.h>
#include "dpu_system.h"
#include "query.h"
#include "x86_process/term_scorer.h"

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value, unsigned int nb_field, char ** field_names);
static bool process_results(dpu_system_t *dpu_system);
static bool save_mram(dpu_system_t *dpu_system, const char* path);

dpu_system_t* initialize_dpu_system(unsigned int nr_dpus, const char* dpu_binary, dpu_type_t dpu_type, char* dpu_profile) {
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

    unsigned int ignored;
    if (dpu_alloc_dpus(&params, nr_dpus, &dpu_system->ranks, &dpu_system->nr_ranks, &ignored) != DPU_API_SUCCESS) {
        fprintf(stderr, "cannot allocate rank\n");
        free(dpu_system);
        return NULL;
    }

    for (unsigned each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        if (dpu_load_all(dpu_system->ranks[each_rank], dpu_binary) != DPU_API_SUCCESS) {
            fprintf(stderr, "cannot load program in dpu\n");
            for (unsigned rank = 0; rank < dpu_system->nr_ranks; ++rank) {
                dpu_free(dpu_system->ranks[rank]);
            }
            free(dpu_system);
            return NULL;
        }
    }

    return dpu_system;
}

void free_dpu_system(dpu_system_t *dpu_system) {
    for (unsigned rank = 0; rank < dpu_system->nr_ranks; ++rank) {
        dpu_free(dpu_system->ranks[rank]);
    }
    free(dpu_system);
}

bool prepare_mram_with_segments(dpu_system_t *dpu_system, unsigned int dpu_idx, mram_image_t *mram_image) {
    dpu_description_t description = dpu_get_description(dpu_system->ranks[0]);
    uint8_t nr_cis = description->topology.nr_of_control_interfaces;
    uint8_t nr_dpus_per_ci = description->topology.nr_of_dpus_per_control_interface;
    uint32_t nr_dpus_per_rank = nr_cis * nr_dpus_per_ci;
    
    uint32_t rank_idx = dpu_idx / nr_dpus_per_rank;
    uint32_t internal_idx = dpu_idx % nr_dpus_per_rank;
    uint32_t ci_idx = internal_idx / nr_dpus_per_ci;
    uint32_t member_idx = internal_idx % nr_dpus_per_ci;
    
    if (dpu_copy_to_dpu(dpu_get(dpu_system->ranks[rank_idx], ci_idx, member_idx), mram_image->content, 0, mram_image->current_offset) != DPU_API_SUCCESS) {
        fprintf(stderr, "error when loading a mram image\n");
        return false;
    }

    return true;
}

bool search(dpu_system_t *dpu_system,
            const char *field,
            const char *value,
            bool save_memory_image,
            unsigned int nb_fields,
            char **field_names) {
    if (!load_query(dpu_system, field, value, nb_fields, field_names)) {
        fprintf(stderr, "error when loading the query\n");
        return false;
    }

    if (save_memory_image && !save_mram(dpu_system, "input.mram.image")) {
        fprintf(stderr, "error when saving the memory image\n");
        return false;
    }

    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        if (dpu_boot_all(rank, ASYNCHRONOUS) != DPU_API_SUCCESS) {
            fprintf(stderr, "error during DPU rank boot\n");
            return false;
        }
    }
    
    unsigned nb_dpu_running;
    do {
        nb_dpu_running = 0;
        for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
            struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
            dpu_run_context_t run_context = dpu_get_run_context(rank);
            nb_dpu_running += run_context->nb_dpu_running;
        }
    } while (nb_dpu_running != 0);

    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        struct dpu_t *dpu;
        DPU_FOREACH(rank, dpu) {
            if (!dpulog_read_for_dpu(dpu, stdout)) {
                fprintf(stderr, "could not read DPU log\n");
            }
        }
    }

    bool fault = false;
    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        dpu_run_context_t run_context = dpu_get_run_context(rank);
        for (int each_ci = 0; each_ci < DPU_MAX_NR_CIS; ++each_ci) {
            if (run_context->dpu_in_fault[each_ci] != 0) {
                for (int each_dpu = 0; each_dpu < sizeof(dpu_bitfield_t) * 8; ++each_dpu) {
                    if ((run_context->dpu_in_fault[each_ci] & (1 << each_dpu)) != 0) {
                        struct dpu_t *dpu = dpu_get(rank, each_ci, each_dpu);
                        fprintf(stderr, "error during DPU execution\n");
                        dpu_gather_postmortem_information(dpu, stdout);
                        fault = true;
                    }
                }
            }
        }
    }

    if (fault) {
        return false;
    }

    if (!process_results(dpu_system)) {
        fprintf(stderr, "error when fetching results\n");
        return false;
    }

    return true;
}

static bool load_query(dpu_system_t *dpu_system, const char *field, const char *value, unsigned int nb_field, char **field_names) {
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
    for (each_field = 0; each_field < nb_field; each_field++) {
        char *field_name = field_names[each_field];
        if (field_name != NULL && strcmp(field, field_name) == 0) {
            query.field_id = each_field;
            break;
        }
    }
    if (query.field_id == -1) {
        fprintf(stderr, "error when looking for field id\n");
        return false;
    }

    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        struct dpu_t *dpu;
        DPU_FOREACH(rank, dpu) {
            if (dpu_copy_to_dpu(dpu, (const uint8_t *)&query, QUERY_BUFFER_OFFSET, sizeof(query)) != DPU_API_SUCCESS) {
                fprintf(stderr, "error when loading the query field\n");
                return false;
            }
        }
    }

    return true;
}

static bool process_results(dpu_system_t *dpu_system) {
    dpu_output_t *results = (dpu_output_t *) malloc(OUTPUTS_BUFFER_SIZE);
    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        struct dpu_t *dpu;
        DPU_FOREACH(rank, dpu) {
            if (dpu_copy_from_dpu(dpu,
                                  OUTPUTS_BUFFER_OFFSET,
                                  (uint8_t *) results,
                                  OUTPUTS_BUFFER_SIZE) != DPU_API_SUCCESS) {
                fprintf(stderr, "error when reading the results\n");
                free(results);
                return false;
            }
            dpu_idf_output_t idf_output;
            if (dpu_copy_from_dpu(dpu, IDF_OUTPUT_OFFSET, (uint8_t *) &idf_output, IDF_OUTPUT_SIZE) !=
                DPU_API_SUCCESS) {
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
        }
    }
    free(results);
    return true;
}

static bool save_mram(dpu_system_t *dpu_system, const char* path) {
    uint8_t *mram = malloc(MRAM_SIZE);
    for (int each_rank = 0; each_rank < dpu_system->nr_ranks; ++each_rank) {
        struct dpu_rank_t *rank = dpu_system->ranks[each_rank];
        struct dpu_t *dpu;
        DPU_FOREACH(rank, dpu) {
            char* complete_path = malloc(strlen(path) + 1 + 9);
            sprintf(complete_path, "%s_%02x_%02x_%02x", path, each_rank, dpu_get_slice_id(dpu), dpu_get_member_id(dpu));
            FILE *file = fopen(complete_path, "wb");

            if (file == NULL) {
                fprintf(stderr, "cannot open file %s\n", complete_path);
                free(mram);
                return false;
            }

            if (dpu_copy_from_dpu(dpu, 0, mram, MRAM_SIZE) != DPU_API_SUCCESS) {
                fprintf(stderr, "error when fetching the mram image\n");
                free(mram);
                fclose(file);
                return false;
            }

            fwrite(mram, 1, MRAM_SIZE, file);
            fclose(file);
        }
    }
    free(mram);
    return true;
}
