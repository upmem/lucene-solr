/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_NORMS_STRUCT_H
#define DPU_POC_NORMS_STRUCT_H

typedef struct {
    int64_t docsWithFieldOffset;
    uint64_t docsWithFieldLength;
    uint16_t jumpTableEntryCount;
    uint8_t denseRankPower;
    uint32_t numDocsWithField;
    uint8_t bytesPerNorm;
    uint64_t normsOffset;
} flat_norms_entry_t;

#endif // DPU_POC_NORMS_STRUCT_H
