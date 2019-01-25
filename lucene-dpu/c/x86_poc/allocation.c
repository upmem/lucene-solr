/*
 * Copyright (c) 2014-2018 - uPmem
 */

#include <stdlib.h>
#include "allocation.h"

void* allocation_get(size_t size) {
    return malloc(size);
}

void allocation_free(void* ptr) {
    free(ptr);
}