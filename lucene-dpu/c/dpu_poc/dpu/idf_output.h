#ifndef __IDF_OUTPUT_H__
#define __IDF_OUTPUT_H__

#include <stdint.h>

void init_idf_output();
void accumulate_idf_output(uint32_t doc_count, uint32_t doc_freq, uint64_t total_term_freq);

#endif /* __IDF_OUTPUT_H__ */
