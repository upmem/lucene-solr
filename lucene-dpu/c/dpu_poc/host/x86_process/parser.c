/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "parser.h"
#include "parser_si.h"
#include "parser_cfe.h"

#include <assert.h>
#include <string.h>
#include <stdlib.h>

#define STRING_MAX_SIZE (512)

static const char *lucene_file_extension[LUCENE_FILE_ENUM_LENGTH] =
        {
                [LUCENE_FILE_SI] = ".si",
                [LUCENE_FILE_CFE] = ".cfe",
                [LUCENE_FILE_CFS] = ".cfs",
                [LUCENE_FILE_FNM] = ".fnm",
                [LUCENE_FILE_FDX] = ".fdx",
                [LUCENE_FILE_FDT] = ".fdt",
                [LUCENE_FILE_TIM] = ".tim",
                [LUCENE_FILE_TIP] = ".tip",
                [LUCENE_FILE_DOC] = ".doc",
                [LUCENE_FILE_POS] = ".pos",
                [LUCENE_FILE_PAY] = ".pay",
                [LUCENE_FILE_NVD] = ".nvd",
                [LUCENE_FILE_NVM] = ".nvm",
                [LUCENE_FILE_DVD] = ".dvd",
                [LUCENE_FILE_DVM] = ".dvm",
                [LUCENE_FILE_TVX] = ".tvx",
                [LUCENE_FILE_TVD] = ".tvd",
                [LUCENE_FILE_LIV] = ".liv",
                [LUCENE_FILE_DII] = ".dii",
                [LUCENE_FILE_DIM] = ".dim",
        };

static lucene_file_e extension_to_index(uint8_t *file_name) {
    unsigned int each;
    for (each = 0; each < LUCENE_FILE_ENUM_LENGTH; each++) {
        size_t file_name_size = strlen(file_name);
        if (!strncmp(&file_name[file_name_size - 3], &lucene_file_extension[each][1], 3))
            return each;
    }
    assert(0 && "FILE WITH UNKNOWN EXTENSION");
}

static FILE *file_open(char *path,
                       unsigned int segment_id,
                       char *filename,
                       lucene_file_e file_type,
                       lucene_si_file_t *si_file) {
    char suffix_filename[STRING_MAX_SIZE];
    sprintf(suffix_filename, "_%u%s\0", segment_id, lucene_file_extension[file_type]);
    check_file_in_si(si_file, suffix_filename);

    sprintf(filename, "%s/%s\0", path, suffix_filename);
    return fopen(filename, "r");
}

static void generate_file_buffers_from_cfs(char *path,
                                           unsigned int segment_id,
                                           char *filename,
                                           file_buffer_t *file_buffers,
                                           lucene_si_file_t *si_file) {
    FILE *f_cfe, *f_cfs;
    lucene_cfe_file_t *cfe_file;
    char suffix_filename[STRING_MAX_SIZE];
    unsigned int each;
    size_t cfs_file_size;
    uint8_t *cfs_buffer;

    f_cfe = file_open(path, segment_id, filename, LUCENE_FILE_CFE, si_file);
    assert(f_cfe != NULL && "CFE FILE NOT FOUND");

    cfe_file = parse_cfe_file(f_cfe);
    /* printf("\n######### CFE_FILE #########\n\n"); */
    /* print_cfe_file(cfe_file); */

    fclose(f_cfe);

    f_cfs = file_open(path, segment_id, filename, LUCENE_FILE_CFS, si_file);
    assert(f_cfs != NULL && "CFS FILE NOT FOUND");

    fseek(f_cfs, 0, SEEK_END);
    cfs_file_size = ftell(f_cfs);
    rewind(f_cfs);

    cfs_buffer = malloc(cfs_file_size);
    fread(cfs_buffer, 1, cfs_file_size, f_cfs);

    fclose(f_cfs);

    for (each = 0; each < cfe_file->NumEntries; each++) {
        unsigned int file_buffers_id = extension_to_index(cfe_file->entries[each].entry_name);
        file_buffers[file_buffers_id].content = malloc(cfe_file->entries[each].length);
        file_buffers[file_buffers_id].length = cfe_file->entries[each].length;
        memcpy(file_buffers[file_buffers_id].content,
               &cfs_buffer[cfe_file->entries[each].offset],
               cfe_file->entries[each].length);
    }

    free(cfs_buffer);
    free_cfe_file(cfe_file);
}

file_buffer_t *get_file_buffers(char *path, unsigned int segment_id) {
    char filename[STRING_MAX_SIZE];
    FILE *f_si;
    lucene_si_file_t *si_file;
    file_buffer_t *file_buffers = calloc(LUCENE_FILE_ENUM_LENGTH, sizeof(*file_buffers));

    sprintf(filename, "%s/_%u%s", path, segment_id, lucene_file_extension[LUCENE_FILE_SI]);
    f_si = fopen(filename, "r");
    assert(f_si != NULL && "SI FILE NOT FOUND");
    si_file = parse_si_file(f_si);
    /* printf("\n######### SI_FILE #########\n\n"); */
    /* print_si_file(si_file); */
    fclose(f_si);

    if (si_file->IsCompoundFile) {
        generate_file_buffers_from_cfs(path, segment_id, filename, file_buffers, si_file);
    } else {
        printf("to be implemented\n");
        assert(0);
    }

    free_si_file(si_file);

    return file_buffers;
}

void free_file_buffers(file_buffer_t *file_buffers) {
    unsigned int each;
    for (each = 0; each < LUCENE_FILE_ENUM_LENGTH; each++) {
        if (file_buffers[each].content != NULL) {
            free(file_buffers[each].content);
        }
    }
    free(file_buffers);
}
