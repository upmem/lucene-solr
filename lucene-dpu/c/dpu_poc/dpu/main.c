/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>

#include "context.h"
#include "search.h"

int main(void) {
    query_t *query;
    flat_search_context_t *context;
    uint32_t task_id = me();

    // initialize_context is long enough for task#0 to initialize the query before any other task needs it
    if (task_id == 0) {
        query = fetch_query(true);
        context = initialize_flat_context(task_id);
    } else {
        context = initialize_flat_context(task_id);
        query = fetch_query(false);
    }

    search(context, query->field, query->value);

    return 0;
}