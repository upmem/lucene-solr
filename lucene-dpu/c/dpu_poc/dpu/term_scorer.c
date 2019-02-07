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

#define K (1.2f)
#define B (0.75f)
static double cache[256] = {
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

float my_log(float a) {
    return a;
}

float compute_score(uint32_t doc_count,
                    uint32_t doc_freq,
                    int32_t freq,
                    uint32_t doc_norm,
                    int64_t total_term_freq) {
    int boost = 1;
    float idf = my_log(1 + (doc_count - doc_freq + 0.5f) / (doc_freq + 0.5f));
    float weight = boost * idf;
    float avgdl = total_term_freq / (float) doc_count;
    float norm = K * ((1 - B) + B * cache[doc_norm & 0xFF] / avgdl);
    return weight * freq / (freq + norm);
}
