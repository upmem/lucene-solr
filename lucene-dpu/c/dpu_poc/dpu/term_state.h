/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERM_STATE_H
#define DPU_POC_TERM_STATE_H

#include <stdint.h>

typedef struct {
    uint32_t doc_freq;
    int32_t total_term_freq;
    uint32_t term_block_ord;
    uint32_t block_file_pointer;

    uint32_t doc_start_fp;
    uint32_t pos_start_fp;
    uint32_t pay_start_fp;
    int32_t singleton_doc_id;
    int32_t last_pos_block_offset;
    int32_t skip_offset;
} term_state_t;

term_state_t *block_term_state_new(void);

#endif //DPU_POC_TERM_STATE_H
