/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_ALLOCATION_H
#define X86_POC_ALLOCATION_H

#include <stddef.h>

void* allocation_get(size_t __size);
void allocation_free(void* ptr);

#endif //X86_POC_ALLOCATION_H
