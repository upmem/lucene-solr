/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>

#include "term_scorer.h"
#include "alloc_wrapper.h"

static term_scorer_t *term_scorer_new(term_weight_t *weight, postings_enum_t *postings_enum, leaf_sim_scorer_t *scorer);

static leaf_sim_scorer_t *leaf_sim_scorer_new(sim_scorer_t *scorer, char *field, bool needs_score);

static inline bool needs_scores(score_mode_t score_mode) {
    switch (score_mode) {
        case SCORE_MODE_COMPLETE:
        case SCORE_MODE_TOP_SCORES:
            return true;
        case SCORE_MODE_COMPLETE_NO_SCORES:
            return false;
    }
}

term_weight_t *build_weight(term_t *term, score_mode_t score_mode, term_state_t *term_state) {
    term_weight_t *result = malloc(sizeof(*result));

    result->term = term;
    result->score_mode = score_mode;
    result->term_state = term_state;

    // todo other fields

    return result;
}

term_scorer_t *build_scorer(term_weight_t *weight,
                            terms_enum_t *terms_enum,
                            mram_reader_t *doc_reader,
                            for_util_t *for_util) {
    term_state_t *term_state = weight->term_state;

    if (term_state == NULL) {
        return NULL;
    }

    // todo in Lucene, terms_enum is fetched from the state, and seeks the term (again, in the simple case)

    leaf_sim_scorer_t *scorer = leaf_sim_scorer_new(weight->sim_scorer, weight->term->field,
                                                    needs_scores(weight->score_mode));

    if (weight->score_mode == SCORE_MODE_TOP_SCORES) {
        return term_scorer_new(weight, impacts(terms_enum, POSTINGS_ENUM_FREQS, doc_reader, for_util), scorer);
    } else {
        abort();
    }
}

static term_scorer_t *term_scorer_new(term_weight_t *weight, postings_enum_t *postings_enum, leaf_sim_scorer_t *scorer) {
    term_scorer_t *result = malloc(sizeof(*result));

    result->weight = weight;
    result->postings_enum = postings_enum;
    result->doc_scorer = scorer;

    return result;
}

static leaf_sim_scorer_t *leaf_sim_scorer_new(sim_scorer_t *scorer, char *field, bool needs_score) {
    leaf_sim_scorer_t *result = malloc(sizeof(*result));

    result->scorer = scorer;
    // todo norms

    return result;
}
