#include <dpu.h>
#include <stdlib.h>
#include <libgen.h>
#include <dirent.h>
#include <string.h>
#include <dpu_characteristics.h>
#include "segment_files.h"
#include "dpu_system.h"

#ifndef DPU_BINARY
#error DPU_BINARY is not defined
#endif

#define STR(x) _STR(x)
#define _STR(x) #x

#define DPU_BINARY_PATH STR(DPU_BINARY)

#define DPU_TYPE FUNCTIONAL_SIMULATOR
#define DPU_PROFILE "cycleAccurate=true"

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <index directory>\n", basename(argv[0]));
        /* fprintf(stderr, "usage: %s <index directory> <segment number>\n", basename(argv[0])); */
        return 1;
    }

    char *index_directory = argv[1];
    /* uint32_t segment_number = (uint32_t) atoi(argv[2]); */

    char** segment_suffixes = NULL;

    DIR* directory = opendir(index_directory);
    struct dirent* file;
    uint32_t current_segment_idx = 0;
    while ((file = readdir(directory))) {
        size_t len = strlen(file->d_name);
        if ((len >= 3) && (strcmp(".si", file->d_name + len - 3) == 0)) {
            segment_suffixes = realloc(segment_suffixes, (current_segment_idx + 1) * sizeof(*segment_suffixes));
            segment_suffixes[current_segment_idx] = malloc(len - 3);
            memcpy(segment_suffixes[current_segment_idx], file->d_name + 1, len - 4);
            segment_suffixes[current_segment_idx][len - 4] = '\0';
            current_segment_idx++;
        }
    }

    uint32_t nr_segments = current_segment_idx;
    uint32_t nr_dpus = (nr_segments / NR_THREADS) + (((nr_segments % NR_THREADS) == 0) ? 0 : 1);
    dpu_system_t *dpu_system = initialize_dpu_system(nr_dpus, DPU_BINARY_PATH, DPU_TYPE, DPU_PROFILE);

    if (dpu_system == NULL) {
        return 2;
    }

    mram_image_t *mram_image = mram_image_new();

    if (mram_image == NULL) {
        free_dpu_system(dpu_system);
        return 3;
    }

    unsigned int nb_fields_per_thread[NR_THREADS];
    char **fields_name_per_thread[NR_THREADS];
    unsigned each_thread;
    unsigned each_dpu;
    current_segment_idx = 0;
    for (each_dpu = 0; each_dpu < nr_dpus; each_dpu++) {
        for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
            if (current_segment_idx == nr_segments) {
                break;
            }

            if (!load_segment_files(mram_image,
                                    index_directory,
                                    each_thread,
                                    segment_suffixes[current_segment_idx],
                                    &nb_fields_per_thread[each_thread],
                                    &fields_name_per_thread[each_thread])) {
                free_mram_image(mram_image);
                free_dpu_system(dpu_system);
                return 4;
            }
            current_segment_idx++;
        }

        if (!prepare_mram_with_segments(dpu_system, each_dpu, mram_image)) {
            free_mram_image(mram_image);
            free_dpu_system(dpu_system);
            return 5;
        }

        mram_image_reset(mram_image);
    }

//    search(dpu_system, "contents", "stupid", true, nb_fields_per_thread, fields_name_per_thread);
    search(dpu_system, "contents", "license", true, nb_fields_per_thread, fields_name_per_thread);
//    search(dpu_system, "contents", "patent", true);
//    search(dpu_system, "contents", "lucene", true);
//    search(dpu_system, "contents", "gnu", true);
//    search(dpu_system, "contents", "derivative", true);
//    search(dpu_system, "contents", "license", true);

    for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
        free (fields_name_per_thread[each_thread]);
    }
    free_mram_image(mram_image);
    free_dpu_system(dpu_system);
    for (int i = 0; i < nr_segments; ++i) {
        free(segment_suffixes[i]);
    }
    free(segment_suffixes);
    return 0;
}
