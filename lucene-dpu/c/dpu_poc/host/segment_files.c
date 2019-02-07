/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include <stdio.h>
#include "segment_files.h"

#define STRING_MAX_SIZE (1024)

static FILE *file_open(const char *path,
                       unsigned int segment_id,
                       const char* extension) {
    char filename[STRING_MAX_SIZE];
    sprintf(filename, "%s/_%u%s", path, segment_id, extension);
    return fopen(filename, "r");
}

static void read_file(FILE* file, uint8_t** buffer, uint32_t *size) {
    fseek(file, 0, SEEK_END);
    *size = ftell(file);
    rewind(file);

    *buffer = malloc(*size);
    fread(*buffer, 1, *size, file);
}

segment_files_t *load_segment_files(const char *index_directory, uint32_t segment_number) {
    segment_files_t *segment_files = malloc(sizeof(*segment_files));

    if (segment_files == NULL) {
        return NULL;
    }

    FILE* si_file = file_open(index_directory, segment_number, ".si");

    if (si_file == NULL) {
        fprintf(stderr, "cannot open si file\n");
        free(segment_files);
        return NULL;
    }
    read_file(si_file, &segment_files->si_file.content, &segment_files->si_file.length);
    fclose(si_file);

    FILE* cfe_file = file_open(index_directory, segment_number, ".cfe");

    if (cfe_file == NULL) {
        fprintf(stderr, "cannot open cfe file\n");
        free(segment_files->si_file.content);
        free(segment_files);
        return NULL;
    }
    read_file(cfe_file, &segment_files->cfe_file.content, &segment_files->cfe_file.length);
    fclose(cfe_file);

    FILE* cfs_file = file_open(index_directory, segment_number, ".cfs");

    if (cfs_file == NULL) {
        fprintf(stderr, "cannot open cfs file\n");
        free(segment_files->si_file.content);
        free(segment_files->cfe_file.content);
        free(segment_files);
        return NULL;
    }
    read_file(cfs_file, &segment_files->cfs_file.content, &segment_files->cfs_file.length);
    fclose(cfs_file);

    return segment_files;
}

void free_segment_files(segment_files_t *segment_files) {
    free(segment_files->si_file.content);
    free(segment_files->cfe_file.content);
    free(segment_files->cfs_file.content);
    free(segment_files);
}