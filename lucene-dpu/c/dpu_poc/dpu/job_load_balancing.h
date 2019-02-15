/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_JOB_LOAD_BALANCING_H
#define DPU_POC_JOB_LOAD_BALANCING_H

#include <stdint.h>
#include <stdbool.h>

typedef struct {
    int32_t doc;
    uint32_t doc_count;
    uint32_t doc_freq;
    int32_t freq;
    uint32_t doc_norm;
    int64_t sum_total_term_freq;
} scoring_job_t;

bool add_scoring_job(int32_t doc,
                     uint32_t doc_count,
                     uint32_t doc_freq,
                     int32_t freq,
                     uint32_t doc_norm,
                     int64_t sum_total_term_freq);

scoring_job_t *fetch_scoring_job(void);
void init_scoring_job_producers(void);
void remove_scoring_job_producer(void);
bool has_job_producers(void);

#endif //DPU_POC_JOB_LOAD_BALANCING_H
