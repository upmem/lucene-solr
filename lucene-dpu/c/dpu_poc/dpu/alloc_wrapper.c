/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <alloc.h>
#include <string.h>
#include "alloc_wrapper.h"

void* malloc(size_t size) {
//    return mem_alloc_dma(size);

    void* ptr = mem_alloc_dma(size);

    memset(ptr, '\0', size);

    return ptr;

}

void* calloc(size_t nmemb, size_t size) {
    size_t num_bytes = nmemb * size;
    void* ptr = malloc(num_bytes);

    memset(ptr, '\0', num_bytes);

    return ptr;
}