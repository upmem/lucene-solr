/*
 * Copyright (c) 2014-2019 - uPmem
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "parser.h"
#include "segment_terms_enum.h"
#include "field_infos.h"
#include "term.h"
#include "allocation.h"
#include "norms.h"
#include "term_scorer.h"

typedef struct {
    file_buffer_t *file_buffers;
    block_tree_term_reader_t *term_reader;
    norms_reader_t *norms_reader;
    field_infos_t *field_infos;

    data_input_t* doc_in;
    for_util_t* for_util;
} lucene_global_context_t;

static block_tree_term_reader_t* build_fields_producer(lucene_global_context_t *ctx);
static data_input_t* build_doc_in(lucene_global_context_t* ctx);

static void search(lucene_global_context_t *ctx, char* field, char* value);

static norms_reader_t *build_norms_producer(lucene_global_context_t *ctx)
{
    field_infos_t* field_infos = ctx->field_infos;

    file_buffer_t* buffer_norms_data = ctx->file_buffers + LUCENE_FILE_NVD;
    file_buffer_t* buffer_norms_metadata = ctx->file_buffers + LUCENE_FILE_NVM;

    data_input_t* norms_data = allocation_get(sizeof(*norms_data));
    norms_data->index = 0;
    norms_data->buffer = buffer_norms_data->content;
    norms_data->read_byte = incremental_read_byte;
    norms_data->skip_bytes = incremental_skip_bytes;

    data_input_t* norms_metadata = allocation_get(sizeof(*norms_metadata));
    norms_metadata->index = 0;
    norms_metadata->buffer = buffer_norms_metadata->content;
    norms_metadata->read_byte = incremental_read_byte;
    norms_metadata->skip_bytes = incremental_skip_bytes;

    return norms_reader_new(norms_metadata, buffer_norms_metadata->length,
                            norms_data, buffer_norms_data->length,
                            field_infos);
}

int main(int argc, char **argv)
{
    lucene_global_context_t ctx;
    ctx.file_buffers = get_file_buffers(argv[1], 0);
    ctx.field_infos = read_field_infos(ctx.file_buffers + LUCENE_FILE_FNM);
    ctx.term_reader = build_fields_producer(&ctx);
    ctx.norms_reader = build_norms_producer(&ctx);
    ctx.doc_in = build_doc_in(&ctx);
    ctx.for_util = build_for_util(ctx.doc_in);

    search(&ctx, "contents", "apache");
    search(&ctx, "contents", "patent");
    search(&ctx, "contents", "lucene");
    search(&ctx, "contents", "gnu");
    search(&ctx, "contents", "derivative");
    search(&ctx, "contents", "license");

    free_file_buffers(ctx.file_buffers);

    return 0;
}

static block_tree_term_reader_t* build_fields_producer(lucene_global_context_t *ctx) {
    field_infos_t* field_infos = ctx->field_infos;

    file_buffer_t* terms_dict = ctx->file_buffers + LUCENE_FILE_TIM;
    file_buffer_t* terms_index = ctx->file_buffers + LUCENE_FILE_TIP;

    data_input_t* terms_in = allocation_get(sizeof(*terms_in));
    terms_in->index = 0;
    terms_in->buffer = terms_dict->content;
    terms_in->read_byte = incremental_read_byte;
    terms_in->skip_bytes = incremental_skip_bytes;

    data_input_t* index_in = allocation_get(sizeof(*index_in));
    index_in->index = 0;
    index_in->buffer = terms_index->content;
    index_in->read_byte = incremental_read_byte;
    index_in->skip_bytes = incremental_skip_bytes;

    return block_tree_term_reader_new(field_infos, terms_in, (uint32_t) terms_dict->length, index_in, (uint32_t) terms_index->length);
}

static data_input_t* build_doc_in(lucene_global_context_t* ctx) {
    file_buffer_t* docs = ctx->file_buffers + LUCENE_FILE_DOC;

    data_input_t* doc_in = allocation_get(sizeof(*doc_in));
    doc_in->index = 0;
    doc_in->buffer = docs->content;
    doc_in->read_byte = incremental_read_byte;
    doc_in->skip_bytes = incremental_skip_bytes;

    check_index_header(doc_in);

    return doc_in;
}

#define K (1.2f)
#define B (0.75f)
static float cache[256] = {
0.0,
1.0,
2.0,
3.0,
4.0,
5.0,
6.0,
7.0,
8.0,
9.0,
10.0,
11.0,
12.0,
13.0,
14.0,
15.0,
16.0,
17.0,
18.0,
19.0,
20.0,
21.0,
22.0,
23.0,
24.0,
25.0,
26.0,
27.0,
28.0,
29.0,
30.0,
31.0,
32.0,
33.0,
34.0,
35.0,
36.0,
37.0,
38.0,
39.0,
40.0,
42.0,
44.0,
46.0,
48.0,
50.0,
52.0,
54.0,
56.0,
60.0,
64.0,
68.0,
72.0,
76.0,
80.0,
84.0,
88.0,
96.0,
104.0,
112.0,
120.0,
128.0,
136.0,
144.0,
152.0,
168.0,
184.0,
200.0,
216.0,
232.0,
248.0,
264.0,
280.0,
312.0,
344.0,
376.0,
408.0,
440.0,
472.0,
504.0,
536.0,
600.0,
664.0,
728.0,
792.0,
856.0,
920.0,
984.0,
1048.0,
1176.0,
1304.0,
1432.0,
1560.0,
1688.0,
1816.0,
1944.0,
2072.0,
2328.0,
2584.0,
2840.0,
3096.0,
3352.0,
3608.0,
3864.0,
4120.0,
4632.0,
5144.0,
5656.0,
6168.0,
6680.0,
7192.0,
7704.0,
8216.0,
9240.0,
10264.0,
11288.0,
12312.0,
13336.0,
14360.0,
15384.0,
16408.0,
18456.0,
20504.0,
22552.0,
24600.0,
26648.0,
28696.0,
30744.0,
32792.0,
36888.0,
40984.0,
45080.0,
49176.0,
53272.0,
57368.0,
61464.0,
65560.0,
73752.0,
81944.0,
90136.0,
98328.0,
106520.0,
114712.0,
122904.0,
131096.0,
147480.0,
163864.0,
180248.0,
196632.0,
213016.0,
229400.0,
245784.0,
262168.0,
294936.0,
327704.0,
360472.0,
393240.0,
426008.0,
458776.0,
491544.0,
524312.0,
589848.0,
655384.0,
720920.0,
786456.0,
851992.0,
917528.0,
983064.0,
1048600.0,
1179672.0,
1310744.0,
1441816.0,
1572888.0,
1703960.0,
1835032.0,
1966104.0,
2097176.0,
2359320.0,
2621464.0,
2883608.0,
3145752.0,
3407896.0,
3670040.0,
3932184.0,
4194328.0,
4718616.0,
5242904.0,
5767192.0,
6291480.0,
6815768.0,
7340056.0,
7864344.0,
8388632.0,
9437208.0,
1.0485784E7,
1.153436E7,
1.2582936E7,
1.3631512E7,
1.4680088E7,
1.5728664E7,
1.677724E7,
1.8874392E7,
2.0971544E7,
2.3068696E7,
2.5165848E7,
2.7263E7,
2.9360152E7,
3.1457304E7,
3.3554456E7,
3.774876E7,
4.1943064E7,
4.6137368E7,
5.0331672E7,
5.4525976E7,
5.872028E7,
6.2914584E7,
6.7108888E7,
7.5497496E7,
8.3886104E7,
9.2274712E7,
1.0066332E8,
1.09051928E8,
1.17440536E8,
1.25829144E8,
1.3421776E8,
1.50994976E8,
1.67772192E8,
1.84549408E8,
2.01326624E8,
2.1810384E8,
2.34881056E8,
2.51658272E8,
2.68435488E8,
3.0198992E8,
3.35544352E8,
3.69098784E8,
4.02653216E8,
4.36207648E8,
4.6976208E8,
5.03316512E8,
5.3687091E8,
6.0397978E8,
6.7108864E8,
7.381975E8,
8.0530637E8,
8.7241523E8,
9.395241E8,
1.00663296E9,
1.07374182E9,
1.20795955E9,
1.34217728E9,
1.47639501E9,
1.61061274E9,
1.74483046E9,
1.87904819E9,
2.01326592E9,
};

static float compute_score(uint32_t doc_count,
                           uint32_t doc_freq,
                           int32_t freq,
                           uint32_t doc_norm,
                           int64_t total_term_freq)
{
    int boost = 1;
    float idf = log( 1 + (doc_count - doc_freq + 0.5f) / (doc_freq + 0.5f));
    float weight = boost * idf;
    float avgdl = total_term_freq / (float)doc_count;
    float norm = K * ( (1-B) + B * cache[doc_norm & 0xFF] / avgdl);
    return weight * freq / (freq + norm);
}

static void search(lucene_global_context_t *ctx, char* field, char* value) {
    term_t* term = term_from_string(field, value);

    field_reader_t* terms = get_terms(ctx->term_reader, term->field);
    segment_terms_enum_t* terms_enum = segment_terms_enum_new(terms);
    bool result = seek_exact(terms_enum, term->bytes);

    if (result) {
        block_term_state_t* term_state = get_term_state(terms_enum);
        int32_t doc_freq = get_doc_freq(terms_enum);
        int64_t total_term_freq = get_total_term_freq(terms_enum);
        printf("MATCH for '%s' (doc_freq=%d, total_term_freq=%ld)\n", term->bytes->bytes, doc_freq, total_term_freq);

        term_weight_t* weight = build_weight(term, SCORE_MODE_TOP_SCORES, 1.0f, term_state);
        term_scorer_t* scorer = build_scorer(weight, terms_enum, ctx->doc_in, ctx->for_util);

        int32_t doc;
        while ((doc = postings_next_doc(scorer->postings_enum)) != NO_MORE_DOCS) {
            printf("doc:%d freq:%d\n", doc, scorer->postings_enum->freq);
            printf("score:%f\n",
                   compute_score(terms->doc_count,
                                 doc_freq,
                                 scorer->postings_enum->freq,
                                 getNorms(ctx->norms_reader, terms->field_info->number, doc),
                                 terms->sum_total_term_freq)
                   );
        }
    } else {
        printf("NO MATCH for '%s'\n", term->bytes->bytes);
    }

}
