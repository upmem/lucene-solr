/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>

#include "perfcounter.h"
#include "mram_structure.h"
#include "search.h"
#include "bytes_ref.h"
#include "bm25_scorer.h"
#include "postings_enum.h"
#include "context.h"
#include "norms.h"
#include "dpu_output.h"
#include "job_load_balancing.h"

static flat_norms_entry_t norms_entries[NR_THREADS];

void search(flat_search_context_t *ctx, uint32_t field_id, char *value, perfcounter_t *perf) {
    unsigned int task_id = me();
    unsigned int nb_output = 0;
    flat_field_reader_t *field_reader = fetch_flat_field_reader(ctx, field_id);
    terms_enum_t *terms_enum = initialize_terms_enum(task_id, field_reader, &ctx->term_reader.terms_in);

    if (seek_exact(terms_enum, bytes_ref_from_string(value))) {
        term_state_t term_state;
        get_term_state(terms_enum, &term_state);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);

        postings_enum_t postings_enum;
        impacts(&postings_enum, terms_enum, POSTINGS_ENUM_FREQS, &ctx->doc_reader, &ctx->for_util);

        flat_norms_entry_t *norms_entry = &norms_entries[task_id];
        mram_readX(flat_context_offsets[task_id]
                   + sizeof(flat_search_context_t)
                   + sizeof(flat_norms_entry_t) * field_reader->field_info.number,
                   norms_entry,
                   sizeof(flat_norms_entry_t));

        dpu_output_t output;
        while ((output.doc_id = postings_next_doc(&postings_enum)) != NO_MORE_DOCS) {
            uint32_t doc_norm = (uint32_t) getNorms(norms_entry, output.doc_id, &ctx->norms_data);

            bool added = add_scoring_job(output.doc_id, field_reader->doc_count, (uint32_t) doc_freq, postings_enum.freq,
                                         doc_norm,
                                         field_reader->sum_total_term_freq, task_id, nb_output);

            if (!added) {
                output.score = compute_bm25(field_reader->doc_count,
                                            (uint32_t) doc_freq,
                                            postings_enum.freq,
                                            doc_norm,
                                            field_reader->sum_total_term_freq);
                MRAM_WRITE(OUTPUTS_BUFFER_OFFSET + task_id * OUTPUTS_BUFFER_SIZE_PER_THREAD + nb_output * OUTPUT_SIZE,
                           &output,
                           OUTPUT_SIZE);
            }

            nb_output++;
        }

        *perf = perfcounter_get();
        remove_scoring_job_producer();

        while (true) {
            scoring_job_t new_job;

            if (fetch_scoring_job(&new_job)) {
                output.doc_id = new_job.doc;
                output.score = compute_bm25(new_job.doc_count,
                                               new_job.doc_freq,
                                               new_job.freq,
                                               new_job.doc_norm,
                                               new_job.sum_total_term_freq);
                MRAM_WRITE(OUTPUTS_BUFFER_OFFSET + new_job.task_id * OUTPUTS_BUFFER_SIZE_PER_THREAD + new_job.output_id * OUTPUT_SIZE,
                           &output,
                           OUTPUT_SIZE);
            } else if (!has_job_producers()) {
                break;
            }
        }
    }

    dpu_output_t last_output = {.score = 0xffffffffffffffffULL, .doc_id = 0xffffffffU};
    MRAM_WRITE(OUTPUTS_BUFFER_OFFSET + task_id * OUTPUTS_BUFFER_SIZE_PER_THREAD + nb_output * OUTPUT_SIZE,
               &last_output,
               OUTPUT_SIZE);
}
