#include "norms.h"
#include "allocation.h"

#include <assert.h>

static bool hasNorms(field_info_t *field_info) {
    return field_info->index_options != INDEX_OPTIONS_NONE && field_info->omit_norms == false;
}

static norms_entry_t **readFields_rec(data_input_t *norms_metadata,
                                      uint32_t max_field_info_number,
                                      field_infos_t *field_infos) {
    int32_t fieldNumber = read_int(norms_metadata);
    norms_entry_t **norms_field_map;
    norms_entry_t *curr_field = allocation_get(sizeof(norms_entry_t));
    field_info_t *field_info;
    uint32_t field_info_number;


    if (fieldNumber == -1) {
        return (norms_entry_t **) allocation_get((max_field_info_number + 1) * sizeof(norms_entry_t *));
    }

    field_info = field_infos->by_number[fieldNumber];
    field_info_number = field_info->number;
    assert(field_info != NULL);
    assert(hasNorms(field_info));

    curr_field->docsWithFieldOffset = read_long(norms_metadata);
    curr_field->docsWithFieldLength = read_long(norms_metadata);
    curr_field->jumpTableEntryCount = read_short(norms_metadata);
    curr_field->denseRankPower = incremental_read_byte(norms_metadata);
    curr_field->numDocsWithField = read_int(norms_metadata);
    curr_field->bytesPerNorm = incremental_read_byte(norms_metadata);
    curr_field->normsOffset = read_long(norms_metadata);

    assert(curr_field->bytesPerNorm == 1
           || curr_field->bytesPerNorm == 2
           || curr_field->bytesPerNorm == 4
           || curr_field->bytesPerNorm == 8);

    if (field_info_number > max_field_info_number) {
        max_field_info_number = field_info_number;
    }

    norms_field_map = readFields_rec(norms_metadata, max_field_info_number, field_infos);

    assert(norms_field_map[field_info_number] == NULL && "norm field already fill!");
    norms_field_map[field_info_number] = curr_field;
    return norms_field_map;
}


norms_reader_t *norms_reader_new(data_input_t *norms_metadata,
                                 uint32_t norms_metadata_length,
                                 data_input_t *norms_data,
                                 uint32_t norms_data_length,
                                 field_infos_t *field_infos) {
    norms_reader_t *reader = allocation_get(sizeof(norms_reader_t));
    lucene_index_header_t *index_header = read_index_header(norms_metadata);
    free_index_header(index_header);

    reader->norms_entries = readFields_rec(norms_metadata, 0, field_infos);

    lucene_codec_footer_t *codec_footer = read_codec_footer(norms_metadata);
    free_codec_footer(codec_footer);

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
        data_input_t *slice = data_input_clone(reader->norms_data);
        slice->skip_bytes(slice, entry->normsOffset);
        slice->skip_bytes(slice, doc * entry->bytesPerNorm);
        switch (entry->bytesPerNorm) {
            case 1:
                return slice->read_byte(slice);
            case 2:
                return read_short(slice);
            case 4:
                return read_int(slice);
            case 8:
                return read_long(slice);
            default:
                assert(0);
        }
    } else {
        assert(0 && "to be implemented\n"); /* Lucene80NormsProducer.java:318 */
    }
}
