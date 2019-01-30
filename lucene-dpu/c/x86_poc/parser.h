/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __PARSER_H__
#define __PARSER_H__

#include <stdint.h>

typedef enum {
    LUCENE_FILE_SI,
    LUCENE_FILE_CFS,
    LUCENE_FILE_CFE,
    LUCENE_FILE_FNM,
    LUCENE_FILE_FDX,
    LUCENE_FILE_FDT,
    LUCENE_FILE_TIM,
    LUCENE_FILE_TIP,
    LUCENE_FILE_DOC,
    LUCENE_FILE_POS,
    LUCENE_FILE_PAY,
    LUCENE_FILE_NVD,
    LUCENE_FILE_NVM,
    LUCENE_FILE_DVD,
    LUCENE_FILE_DVM,
    LUCENE_FILE_TVX,
    LUCENE_FILE_TVD,
    LUCENE_FILE_LIV,
    LUCENE_FILE_DII,
    LUCENE_FILE_DIM,
    LUCENE_FILE_ENUM_LENGTH,
} lucene_file_e;

typedef struct {
    uint64_t length;
    uint8_t *content;
} file_buffer_t;

file_buffer_t *get_file_buffers(char *path, unsigned int segment_id);

void free_file_buffers(file_buffer_t *file_buffers);

#endif /* __PARSER_H__ */
