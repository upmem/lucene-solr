/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stddef.h>
#include <mutex.h>
#include "dpu_characteristics.h"
#include "job_load_balancing.h"

#define MAX_NR_JOBS NR_THREADS

uint32_t nr_producers;
uint32_t nr_jobs_available;

scoring_job_t jobs[MAX_NR_JOBS];

mutex_t job_mutex = mutex_get(0);
mutex_t producer_mutex = mutex_get(1);

bool add_scoring_job(int32_t doc,
                     uint32_t doc_count,
                     uint32_t doc_freq,
                     int32_t freq,
                     uint32_t doc_norm,
                     int64_t sum_total_term_freq,
                     uint32_t task_id,
                     uint32_t output_id) {
    mutex_lock(job_mutex);

    uint32_t job_id;

    if ((job_id = nr_jobs_available) == MAX_NR_JOBS) {
        mutex_unlock(job_mutex);
        return false;
    }

    jobs[job_id].doc = doc;
    jobs[job_id].doc_count = doc_count;
    jobs[job_id].doc_freq = doc_freq;
    jobs[job_id].freq = freq;
    jobs[job_id].doc_norm = doc_norm;
    jobs[job_id].sum_total_term_freq = sum_total_term_freq;
    jobs[job_id].task_id = task_id;
    jobs[job_id].output_id = output_id;

    nr_jobs_available++;

    mutex_unlock(job_mutex);

    return true;
}

scoring_job_t *fetch_scoring_job(void) {
    mutex_lock(job_mutex);

    uint32_t job_id;

    if ((job_id = nr_jobs_available) == 0) {
        mutex_unlock(job_mutex);
        return NULL;
    }

    nr_jobs_available--;

    mutex_unlock(job_mutex);
    return jobs + (job_id - 1);
}

void init_scoring_job_producers(void) {
    nr_producers = NR_THREADS;
}

void remove_scoring_job_producer(void) {
    mutex_lock(producer_mutex);
    nr_producers--;
    mutex_unlock(producer_mutex);
}

bool has_job_producers(void) {
    mutex_lock(producer_mutex);
    bool non_empty = nr_producers != 0;
    mutex_unlock(producer_mutex);

    return non_empty;
}