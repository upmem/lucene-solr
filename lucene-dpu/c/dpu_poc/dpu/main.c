/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>

#include "context.h"
#include "search.h"

int main(void) {
    query_t *query;
    search_context_t *context;
    terms_enum_t *terms_enum;
    uint32_t task_id = me();

    // initialize_context is long enough for task#0 to initialize the query before any other task needs it
    if (task_id == 0) {
        query = fetch_query(true);
        context = initialize_context(task_id);
    } else {
        context = initialize_context(task_id);
        query = fetch_query(false);
    }

    terms_enum = initialize_terms_enum(task_id);
    search(context, terms_enum, query->field, query->value);

    return 0;
}