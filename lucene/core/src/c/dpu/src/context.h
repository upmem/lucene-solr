/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_INIT_H
#define DPU_POC_INIT_H

#include "query.h"
#include "search_context.h"
#include "terms_enum.h"
#include <stdbool.h>
#include <stdint.h>

extern uint64_t flat_context_offsets[NR_TASKLETS];

query_t *fetch_query(bool do_init);

flat_search_context_t *initialize_flat_context(uint32_t index);
flat_field_reader_t *fetch_flat_field_reader(flat_search_context_t *context, const uint32_t field);
terms_enum_t *initialize_terms_enum(uint32_t index, flat_field_reader_t *field_reader, mram_reader_t *terms_in);

#endif // DPU_POC_INIT_H
