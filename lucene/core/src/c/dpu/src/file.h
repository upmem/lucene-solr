/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_FILE_H
#define DPU_POC_FILE_H

#include <mram.h>

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
    uint32_t length;
    mram_addr_t offset;
} file_buffer_t;

#endif // DPU_POC_FILE_H
