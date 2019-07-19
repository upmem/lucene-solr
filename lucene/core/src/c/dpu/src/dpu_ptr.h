/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_DPU_PTR_H
#define DPU_POC_DPU_PTR_H

#ifdef DPU
#define DPU_PTR(ptr) ptr
#define DPU_NULL NULL
#else
#define DPU_PTR(ptr) uint32_t
#define DPU_NULL 0
#endif

#endif // DPU_POC_DPU_PTR_H
