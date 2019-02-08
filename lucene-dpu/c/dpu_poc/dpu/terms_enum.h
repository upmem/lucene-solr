/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef DPU_POC_TERMS_ENUM_H
#define DPU_POC_TERMS_ENUM_H

#include <stdbool.h>
#include "term_reader.h"
#include "fst.h"
#include "term_state.h"
#include "terms_enum_struct.h"

bool seek_exact(terms_enum_t *terms_enum, bytes_ref_t *target);
void init_index_input(terms_enum_t *terms_enum);

terms_enum_frame_t *push_frame_fp(terms_enum_t *terms_enum, uint32_t fp, uint32_t length);

void get_term_state(terms_enum_t *terms_enum, term_state_t *term_state);

int32_t get_doc_freq(terms_enum_t *terms_enum);

int64_t get_total_term_freq(terms_enum_t *terms_enum);

#endif //DPU_POC_TERMS_ENUM_H
