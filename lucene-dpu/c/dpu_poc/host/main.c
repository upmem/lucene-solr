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
#define DPU_PROFILE "iramSize=8192"

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <index directory> <segment number>\n", basename(argv[0]));
        return 1;
    }

    char *index_directory = argv[1];
    uint32_t segment_number = (uint32_t) atoi(argv[2]);

    dpu_system_t *dpu_system = initialize_dpu_system(DPU_BINARY_PATH, DPU_TYPE, DPU_PROFILE);

    if (dpu_system == NULL) {
        return 1;
    }

    mram_image_t *mram_image = mram_image_new(NR_THREADS);

    if (mram_image == NULL) {
        free_dpu_system(dpu_system);
        return 1;
    }

    if (!load_segment_files(mram_image, index_directory, segment_number)) {
        free_mram_image(mram_image);
        free_dpu_system(dpu_system);
        return 1;
    }

    if (!prepare_mrams_with_segments(dpu_system, mram_image)) {
        free_mram_image(mram_image);
        free_dpu_system(dpu_system);
        return 1;
    }

    free_mram_image(mram_image);

    search(dpu_system, "contents", "apache", true);
//    search(dpu_system, "contents", "patent", true);
//    search(dpu_system, "contents", "lucene", true);
//    search(dpu_system, "contents", "gnu", true);
//    search(dpu_system, "contents", "derivative", true);
//    search(dpu_system, "contents", "license", true);

    free_dpu_system(dpu_system);

    return 0;
}