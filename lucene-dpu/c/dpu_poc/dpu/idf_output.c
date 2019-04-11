#include "mutex.h"
#include "dpu_output.h"
#include "dpu_characteristics.h"
#include "mram_structure.h"
#include "mram_access.h"

DECLARE_MUTEX(idf_mutex);
static unsigned nb_thread_accumulated;
static dpu_idf_output_t idf_output;
static mutex_t idf_mutex;

void init_idf_output()
{
    idf_mutex = MUTEX(idf_mutex);
    nb_thread_accumulated = 0;
    idf_output.doc_count = 0;
    idf_output.doc_freq = 0;
    idf_output.total_term_freq = 0ULL;
}

void accumulate_idf_output(uint32_t doc_count, uint32_t doc_freq, uint64_t total_term_freq)
{
    mutex_lock(idf_mutex);

    idf_output.doc_count += doc_count;
    idf_output.doc_freq += doc_freq;
    idf_output.total_term_freq += total_term_freq;
    nb_thread_accumulated ++;

    mutex_unlock(idf_mutex);

    if (nb_thread_accumulated == NR_THREADS) {
        MRAM_WRITE(IDF_OUTPUT_OFFSET, &idf_output, IDF_OUTPUT_SIZE);
    }
}
