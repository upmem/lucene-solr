/*
 * Copyright (c) 2014-2018 - uPmem
 */

#ifndef X86_POC_SEGMENT_TERMS_ENUM_H
#define X86_POC_SEGMENT_TERMS_ENUM_H

#include <stdbool.h>
#include "bytes_ref.h"
#include "data_input.h"

typedef struct {

} segment_term_enum_t;

bool seek_exact(segment_term_enum_t* terms_enum, bytes_ref_t target);

#endif //X86_POC_SEGMENT_TERMS_ENUM_H
