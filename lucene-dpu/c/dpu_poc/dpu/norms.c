#include <stdlib.h>

#include "norms.h"
#include "alloc_wrapper.h"

static bool hasNorms(field_info_t *field_info) {
    return field_info->index_options != INDEX_OPTIONS_NONE && field_info->omit_norms == false;
}

static norms_entry_t **readFields_rec(mram_reader_t *norms_metadata,
                                      uint32_t max_field_info_number,
                                      field_infos_t *field_infos) {
    int32_t fieldNumber = mram_read_int(norms_metadata, false);
    norms_entry_t **norms_field_map;
    norms_entry_t *curr_field;
    field_info_t *field_info;
    uint32_t field_info_number;


    if (fieldNumber == -1) {
        return (norms_entry_t **) malloc((max_field_info_number + 1) * sizeof(norms_entry_t *));
    }
    curr_field = malloc(sizeof(norms_entry_t));

    field_info = field_infos->by_number[fieldNumber];
    field_info_number = field_info->number;
    if (field_info == NULL || !hasNorms(field_info)) {
        abort();
    }

    curr_field->docsWithFieldOffset = mram_read_long(norms_metadata, false);
    curr_field->docsWithFieldLength = mram_read_long(norms_metadata, false);
    curr_field->jumpTableEntryCount = mram_read_short(norms_metadata, false);
    curr_field->denseRankPower = mram_read_byte(norms_metadata, false);
    curr_field->numDocsWithField = mram_read_int(norms_metadata, false);
    curr_field->bytesPerNorm = mram_read_byte(norms_metadata, false);
    curr_field->normsOffset = mram_read_long(norms_metadata, false);

    if (!(curr_field->bytesPerNorm == 1
          || curr_field->bytesPerNorm == 2
          || curr_field->bytesPerNorm == 4
          || curr_field->bytesPerNorm == 8)) {
        abort();
    }

    if (field_info_number > max_field_info_number) {
        max_field_info_number = field_info_number;
    }

    norms_field_map = readFields_rec(norms_metadata, max_field_info_number, field_infos);

    if (norms_field_map[field_info_number] != NULL) {
        abort();
    }
    norms_field_map[field_info_number] = curr_field;
    return norms_field_map;
}


norms_reader_t *norms_reader_new(mram_reader_t *norms_metadata,
                                 uint32_t norms_metadata_length,
                                 mram_reader_t *norms_data,
                                 uint32_t norms_data_length,
                                 field_infos_t *field_infos) {
    norms_reader_t *reader = malloc(sizeof(norms_reader_t));
    index_header_t *index_header = read_index_header(norms_metadata);

    reader->norms_entries = readFields_rec(norms_metadata, 0, field_infos);

    codec_footer_t *codec_footer = read_codec_footer(norms_metadata);

    reader->norms_data = norms_data;
    reader->norms_data_length = norms_data_length;

    return reader;
}

uint64_t getNorms(norms_reader_t *reader, uint32_t field_number, int doc) {
    norms_entry_t *entry = reader->norms_entries[field_number];

    if (entry->docsWithFieldOffset == -2) {
        return 0;
    } else if (entry->docsWithFieldOffset == -1) {
        if (entry->bytesPerNorm == 0) {
            return entry->normsOffset;
        }
        mram_reader_t *slice = mram_reader_clone(reader->norms_data);
        mram_skip_bytes(slice, entry->normsOffset, false);
        mram_skip_bytes(slice, doc * entry->bytesPerNorm, false);
        switch (entry->bytesPerNorm) {
            case 1:
                return mram_read_byte(slice, false);
            case 2:
                return mram_read_short(slice, false);
            case 4:
                return mram_read_int(slice, false);
            case 8:
                return mram_read_long(slice, false);
            default:
                abort();
        }
    } else {
        abort();
    }
}
