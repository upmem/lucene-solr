/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>

#include <ktrace.h>

#include "search.h"
#include "bytes_ref.h"
#include "bm25_scorer.h"
#include "postings_enum.h"
#include "context.h"
#include "norms.h"

void search(flat_search_context_t *ctx, char* field, char *value) {
    flat_field_reader_t *field_reader = fetch_flat_field_reader(ctx, field);
    terms_enum_t *terms_enum = initialize_terms_enum(me(), field_reader, &ctx->term_reader.terms_in);

    if (seek_exact(terms_enum, bytes_ref_from_string(value))) {
        term_state_t term_state;
        get_term_state(terms_enum, &term_state);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);

        postings_enum_t postings_enum;
        impacts(&postings_enum, terms_enum, POSTINGS_ENUM_FREQS, &ctx->doc_reader, &ctx->for_util);
        flat_norms_entry_t* norms_entry = ctx->entries + field_reader->field_info.number;

        int32_t doc;
        while ((doc = postings_next_doc(&postings_enum)) != NO_MORE_DOCS) {
            long long score = compute_bm25(field_reader->doc_count,
                                           (uint32_t) doc_freq,
                                           postings_enum.freq,
                                           getNorms(norms_entry, doc, &ctx->norms_data),
                                           field_reader->sum_total_term_freq);
            ktrace("doc:%d freq:%d score:%i\n", doc, postings_enum.freq, (int) score);
        }
    }
}
