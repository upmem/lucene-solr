/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include "allocation.h"

void* allocation_get(size_t size) {
    return calloc(1, size);
}

void allocation_free(void* ptr) {
    free(ptr);
}