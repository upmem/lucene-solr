/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <perfcounter.h>
#include <ktrace.h>

#include "context.h"
#include "search.h"
#include "job_load_balancing.h"

int main(void) {
    query_t *query;
    flat_search_context_t *context;
    uint32_t task_id = me();
    perfcounter_t start, end;

    // initialize_context is long enough for task#0 to initialize the query before any other task needs it
    if (task_id == 0) {
        init_scoring_job_producers();
        query = fetch_query(true);
        perfcounter_config(COUNT_CYCLES, true);
        context = initialize_flat_context(task_id);
    } else {
        context = initialize_flat_context(task_id);
        query = fetch_query(false);
    }

    start = perfcounter_get();
    search(context, query->field_id, query->value);
    end = perfcounter_get();

    ktrace("[%i] perfcounter:%u\n", task_id, (unsigned)(end - start));

    return 0;
}
