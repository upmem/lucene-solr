#ifndef __NORMS_H__
#define __NORMS_H__

#include <stdint.h>
#include "parser_index_header.h"
#include "parser_codec_footer.h"
#include "data_input.h"
#include "field_infos.h"

typedef struct {
    int64_t docsWithFieldOffset;
    uint64_t docsWithFieldLength;
    uint16_t jumpTableEntryCount;
    uint8_t denseRankPower;
    uint32_t numDocsWithField;
    uint8_t bytesPerNorm;
    uint64_t normsOffset;
} norms_entry_t;

typedef struct {
    norms_entry_t **norms_entries;
    data_input_t *norms_data;
    uint32_t norms_data_length;
} norms_reader_t;

norms_reader_t *norms_reader_new(data_input_t *norms_metadata,
                                 uint32_t norms_metadata_length,
                                 data_input_t *norms_data,
                                 uint32_t norms_data_length,
                                 field_infos_t *field_infos);

uint64_t getNorms(norms_reader_t *reader, uint32_t field_number, int doc);

#endif /* __NORMS_H__ */
