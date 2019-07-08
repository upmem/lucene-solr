#include <dpu.h>
#include <stdlib.h>
#include <libgen.h>
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
    if (argc != 4) {
        fprintf(stderr, "usage: %s <index directory> <field name> <term search>\n", basename(argv[0]));
        return 1;
    }

    char *index_directory = argv[1];
    char *field = argv[2];
    char *term = argv[3];

    char** segment_suffixes = NULL;
    uint32_t nr_segments = discover_segments(index_directory, &segment_suffixes);

    if (nr_segments == -1) {
        return 1;
    }

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

    unsigned int nb_fields_per_segment = -1;
    char **fields_name_per_segment = NULL;
    unsigned each_thread;
    unsigned each_dpu;
    unsigned current_segment_idx = 0;
    for (each_dpu = 0; each_dpu < nr_dpus; each_dpu++) {
        for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
            if (current_segment_idx == nr_segments) {
                for (int each_empty_thread = each_thread; each_empty_thread < NR_THREADS; ++each_empty_thread) {
                    load_empty_segment(mram_image, each_empty_thread);
                }
                break;
            }

            if (!load_segment_files(mram_image,
                                    index_directory,
                                    each_thread,
                                    segment_suffixes[current_segment_idx],
                                    &nb_fields_per_segment,
                                    &fields_name_per_segment)) {
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

    for (each_thread = 0; each_thread < NR_THREADS; each_thread++) {
        load_empty_segment(mram_image, each_thread);
    }

    for (each_dpu = nr_dpus; each_dpu < dpu_system->nr_dpus; ++each_dpu) {
        if (!prepare_mram_with_segments(dpu_system, each_dpu, mram_image)) {
            free_mram_image(mram_image);
            free_dpu_system(dpu_system);
            return 5;
        }
    }

    search(dpu_system, field, term, true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "stupid", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "license", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "patent", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "lucene", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "gnu", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "derivative", true, nb_fields_per_segment, fields_name_per_segment);
//    search(dpu_system, "contents", "license", true, nb_fields_per_segment, fields_name_per_segment);

    if (fields_name_per_segment != NULL) {
        for (int each_field = 0; each_field < nb_fields_per_segment; ++each_field) {
            free(fields_name_per_segment[each_field]);
        }
        free(fields_name_per_segment);
    }
    free_mram_image(mram_image);
    free_dpu_system(dpu_system);
    for (int i = 0; i < nr_segments; ++i) {
        free(segment_suffixes[i]);
    }
    free(segment_suffixes);
    return 0;
}
