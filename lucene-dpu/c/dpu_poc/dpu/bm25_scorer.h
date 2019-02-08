#ifndef __BM25_SCORER_H__
#define __BM25_SCORER_H__

long long compute_bm25(uint32_t doc_count,
                       uint32_t doc_freq,
                       int32_t freq,
                       uint32_t doc_norm,
                       int64_t total_term_freq);

#endif /* __BM25_SCORER_H__ */
