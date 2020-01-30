/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include "alloc_wrapper.h"
#include <alloc.h>
#include <string.h>

void *malloc(size_t size)
{
    // todo fix unitialized values
    return mem_alloc(size);

    //    void* ptr = mem_alloc_dma(size);
    //    memset(ptr, '\0', size);
    //    return ptr;
}
