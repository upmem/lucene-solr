/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <perfcounter.h>
#include <ktrace.h>
#include <barrier.h>

#include "context.h"
#include "search.h"
#include "job_load_balancing.h"

#define TASKLETS_INITIALIZER \
    TASKLETS(NR_THREADS, main, 1024, 0)
#include <rt.h>

DECLARE_BARRIER(init_barrier, NR_THREADS)

int main(void) {
    query_t *query;
    flat_search_context_t *context;
    uint32_t task_id = me();
    perfcounter_t start, end, middle;
    barrier_t barrier = BARRIER(init_barrier);

    if (task_id == 0) {
        init_scoring_job_producers();
        perfcounter_config(COUNT_CYCLES, true);
        query = fetch_query(true);
    }

    context = initialize_flat_context(task_id);

    barrier_wait(barrier);

    if (task_id != 0) {
        query = fetch_query(false);
    }

    barrier_wait(barrier);

    start = perfcounter_get();
    search(context, query->field_id, query->value, &middle);
    end = perfcounter_get();

    ktrace("[%i] perfcounter:%u (seek %u, bm25 %u)\n",
           task_id,
           (unsigned)(end - start),
           (unsigned)(middle - start),
           (unsigned)(end - middle));

    return 0;
}
