/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __DPU_OUTPUT_H__
#define __DPU_OUTPUT_H__

#define SCORE_PRECISION_SHIFT (10)
#define SCORE_PRECISION (1 << SCORE_PRECISION_SHIFT)

typedef struct {
    uint32_t doc_id;
    uint32_t unused;
    uint32_t freq;
    uint32_t doc_norm;
} dpu_output_t;

typedef struct {
    uint32_t doc_count;
    uint32_t doc_freq;
    uint64_t total_term_freq;
} dpu_idf_output_t;

#endif /*__DPU_OUTPUT_H__*/
