/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_SEARCH_H
#define DPU_POC_SEARCH_H

#include "context.h"

void search(flat_search_context_t *ctx, uint32_t field, char *value);
void no_search();

#endif //DPU_POC_SEARCH_H
