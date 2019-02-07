#ifndef __NORMS_H__
#define __NORMS_H__

#include <stdint.h>
#include "index_header.h"
#include "codec_footer.h"
#include "mram_reader.h"
#include "field_info.h"

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
    mram_reader_t *norms_data;
    uint32_t norms_data_length;
} norms_reader_t;

norms_reader_t *norms_reader_new(mram_reader_t *norms_metadata,
                                 uint32_t norms_metadata_length,
                                 mram_reader_t *norms_data,
                                 uint32_t norms_data_length,
                                 field_infos_t *field_infos);

uint64_t getNorms(norms_reader_t *reader, uint32_t field_number, int doc);

#endif /* __NORMS_H__ */
