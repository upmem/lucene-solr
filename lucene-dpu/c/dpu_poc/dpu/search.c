/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <stdlib.h>
#include "search.h"
#include "term_reader.h"
#include "term_scorer.h"
#include "bytes_ref.h"

void search(search_context_t *ctx, terms_enum_t *terms_enum, char* field, char *value) {
    term_t *term = term_from_string(field, value);
    field_reader_t *field_reader = fetch_field_reader(ctx->term_reader, field);
    terms_enum->field_reader = field_reader;

    if (field_reader->index == NULL) {
        terms_enum->fst_reader = NULL;
    } else {
        terms_enum->fst_reader = fst_get_bytes_reader(field_reader->index);
    }

    if (seek_exact(terms_enum, bytes_ref_from_string(value))) {
        term_state_t *term_state = get_term_state(terms_enum);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);

        term_weight_t *weight = build_weight(term, SCORE_MODE_TOP_SCORES, term_state);
        term_scorer_t *scorer = build_scorer(weight, terms_enum, ctx->doc_reader, ctx->for_util);

        int32_t doc;
        while ((doc = postings_next_doc(scorer->postings_enum)) != NO_MORE_DOCS) {
        }
    }
}
