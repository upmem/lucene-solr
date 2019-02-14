/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef __DPU_OUTPUTS_H__
#define __DPU_OUTPUTS_H__

#define SCORE_PRECISION_SHIFT (10)
#define SCORE_PRECISION (1 << SCORE_PRECISION_SHIFT)

typedef struct {
    uint64_t score;
    uint32_t doc_id;
    uint32_t unused;
} dpu_outputs_t;

#endif /*__DPU_OUTPUTS_H__*/
