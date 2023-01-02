/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <defs.h>
#include <stdlib.h>
#include <mutex.h>

#include "bytes_ref.h"
#include "context.h"
#include "dpu_output.h"
#include "idf_output.h"
#include "mram_structure.h"
#include "norms.h"
#include "perfcounter.h"
#include "postings_enum.h"
#include "search.h"

MUTEX_INIT(output_mutex);

static flat_norms_entry_t norms_entries[NR_TASKLETS];

static void finalize_results(uint32_t my_next_outputs, uint32_t output_cnt)
{
  static __dma_aligned dpu_output_t last_output = { .doc_id = 0xffffffffU };
  while(output_cnt < OUTPUTS_PER_TASKLET_BATCH) {
    mram_write(&last_output,
        (__mram_ptr void *)(OUTPUTS_BUFFER_OFFSET + (my_next_outputs + output_cnt) * OUTPUT_SIZE),
        OUTPUT_SIZE);
    output_cnt++;
  }
}

void search(flat_search_context_t *ctx, uint32_t field_id, char *value, uint32_t *nb_output)
{
    unsigned int task_id = me();
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
        mram_read((__mram_ptr void *)(flat_context_offsets[task_id] + sizeof(flat_search_context_t)
                      + sizeof(flat_norms_entry_t) * field_reader->field_info.number),
            norms_entry, sizeof(flat_norms_entry_t));

        accumulate_idf_output(field_reader->doc_count, doc_freq, field_reader->sum_total_term_freq);

        dpu_output_t output;
        output.tid = me();
        unsigned int my_next_outputs = 0;
        int output_cnt = OUTPUTS_PER_TASKLET_BATCH;
        while ((output.doc_id = postings_next_doc(&postings_enum)) != NO_MORE_DOCS) {

            output.freq = postings_enum.freq;
            output.doc_norm = (uint32_t)getNorms(norms_entry, output.doc_id, &ctx->norms_data);

            if(output_cnt == OUTPUTS_PER_TASKLET_BATCH) {
              // allocate new outputs in global buffer
              mutex_lock(output_mutex);
              my_next_outputs = *nb_output;
              *nb_output+=OUTPUTS_PER_TASKLET_BATCH;
              mutex_unlock(output_mutex);
              output_cnt = 0;
            }
            mram_write(&output,
                (__mram_ptr void *)(OUTPUTS_BUFFER_OFFSET + (my_next_outputs + output_cnt) * OUTPUT_SIZE),
                OUTPUT_SIZE);
            output_cnt++;
        }

        finalize_results(my_next_outputs, output_cnt);
    }
    else {
      accumulate_idf_output(0, 0, 0);
    }
}
