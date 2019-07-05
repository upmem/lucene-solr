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
    uint32_t idx = 0;
    while ((file = readdir(directory))) {
        size_t len = strlen(file->d_name);
        if ((len >= 3) && (strcmp(".si", file->d_name + len - 3) == 0)) {
            segment_suffixes = realloc(segment_suffixes, (idx + 1) * sizeof(*segment_suffixes));
            segment_suffixes[idx] = malloc(len - 3);
            memcpy(segment_suffixes[idx], file->d_name + 1, len - 4);
            segment_suffixes[idx][len - 4] = '\0';
            idx++;
        }
    }

    dpu_system_t *dpu_system = initialize_dpu_system(DPU_BINARY_PATH, DPU_TYPE, DPU_PROFILE);

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
    for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
        if (!load_segment_files(mram_image,
                                index_directory,
                                each_thread,
                                segment_suffixes[each_thread],
                                &nb_fields_per_thread[each_thread],
                                &fields_name_per_thread[each_thread])) {
            free_mram_image(mram_image);
            free_dpu_system(dpu_system);
            return 4;
        }
    }

    if (!prepare_mrams_with_segments(dpu_system, mram_image)) {
        free_mram_image(mram_image);
        free_dpu_system(dpu_system);
        return 5;
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
    for (int i = 0; i < idx; ++i) {
        free(segment_suffixes[i]);
    }
    free(segment_suffixes);
    return 0;
}
