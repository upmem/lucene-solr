/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_TERM_SCORER_H
#define X86_POC_TERM_SCORER_H

#include <stdint.h>
#include "segment_terms_enum.h"
#include "term.h"

typedef struct {

} sim_scorer_t;

typedef struct {
    sim_scorer_t* scorer;
    // todo norms
} leaf_sim_scorer_t;

typedef struct {
    term_t* term;
} query_t;

typedef enum {
    SCORE_MODE_COMPLETE,
    SCORE_MODE_COMPLETE_NO_SCORES,
    SCORE_MODE_TOP_SCORES
} score_mode_t;

typedef struct {
    query_t* query;
    // todo similarity
    sim_scorer_t* sim_scorer;
    block_term_state_t* term_state; // todo this is a TermStates in Lucene
    score_mode_t score_mode;
} term_weight_t;

typedef struct {
    term_weight_t* weight;
    postings_enum_t* postings_enum;
    // todo impactsEnum
    // todo iterator
    leaf_sim_scorer_t* doc_scorer;
    // todo impactsDisi
} term_scorer_t;

term_weight_t* build_weight(query_t* query, score_mode_t score_mode, float boost, block_term_state_t* term_state);
term_scorer_t* build_scorer(term_weight_t* weight, segment_terms_enum_t* terms_enum, data_input_t* doc_in, for_util_t* for_util);

#endif //X86_POC_TERM_SCORER_H
