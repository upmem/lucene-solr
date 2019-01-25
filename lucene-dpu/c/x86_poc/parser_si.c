#include "parser_si.h"
#include "parser_index_header.h"
#include "parser_codec_footer.h"
#include "allocation.h"
#include "data_input.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

lucene_si_file_t *parse_si_file(FILE *f_si)
{
    lucene_si_file_t *si_file;
    size_t si_file_size;
    data_input_t buffer =
        {
         .index = 0,
         .read_byte = incremental_read_byte,
         .skip_bytes = incremental_skip_bytes,
        };
    unsigned int each;
    uint32_t unused_length;

    fseek(f_si, 0, SEEK_END);
    si_file_size = ftell(f_si);
    rewind(f_si);

    buffer.buffer = malloc(si_file_size);
    fread(buffer.buffer, 1, si_file_size, f_si);

    si_file = allocation_get(sizeof(lucene_si_file_t));

    si_file->index_header = read_index_header(&buffer);

    si_file->SegVersion[0] = read_int(&buffer);
    si_file->SegVersion[1] = read_int(&buffer);
    si_file->SegVersion[2] = read_int(&buffer);

    si_file->HasMinVersion = incremental_read_byte(&buffer);

    if (si_file->HasMinVersion) {
        si_file->MinVersion[0] = read_int(&buffer);
        si_file->MinVersion[1] = read_int(&buffer);
        si_file->MinVersion[2] = read_int(&buffer);
    }

    si_file->DocCount = read_int(&buffer);

    si_file->IsCompoundFile = incremental_read_byte(&buffer);

    si_file->DiagnosticsCount = read_vint(&buffer);
    si_file->Diagnostics = (uint8_t ***)allocation_get(si_file->DiagnosticsCount * sizeof(uint8_t **));
    for (each = 0; each < si_file->DiagnosticsCount; each++) {
        si_file->Diagnostics[each] = (uint8_t **)allocation_get(2 * sizeof(uint8_t *));

        si_file->Diagnostics[each][0] = read_string(&buffer, &unused_length);
        si_file->Diagnostics[each][1] = read_string(&buffer, &unused_length);
    }

    si_file->FilesCount = read_vint(&buffer);
    si_file->Files = (uint8_t **)allocation_get(si_file->FilesCount * sizeof(uint8_t *));
    for (each = 0; each < si_file->FilesCount; each++) {
        si_file->Files[each] = read_string(&buffer, &unused_length);
    }

    si_file->AttributesCount = read_vint(&buffer);
    si_file->Attributes = (uint8_t ***)allocation_get(si_file->AttributesCount * sizeof(uint8_t **));
    for (each = 0; each < si_file->AttributesCount; each++) {
        si_file->Attributes[each] = (uint8_t **)allocation_get(2 * sizeof(uint8_t *));

        si_file->Attributes[each][0] = read_string(&buffer, &unused_length);
        si_file->Attributes[each][1] = read_string(&buffer, &unused_length);
    }

    si_file->NumSortFields = read_vint(&buffer);

    si_file->codec_footer = read_codec_footer(&buffer);

    free(buffer.buffer);
    return si_file;
}

void print_si_file(lucene_si_file_t *si_file)
{
    unsigned int each;
    print_index_header(si_file->index_header);

    printf("SegVersion: %i.%i.%i\n"
           "HasMinVersion: %u\n",
           si_file->SegVersion[0], si_file->SegVersion[1], si_file->SegVersion[2],
           si_file->HasMinVersion);
    if (si_file->HasMinVersion) {
        printf("MinVersion: %i.%i.%i\n",
               si_file->MinVersion[0], si_file->MinVersion[1], si_file->MinVersion[2]);
    }
    printf("DocCount: %i\n"
           "IsCompoundFile %u\n"
           "MapCount %u\n",
           si_file->DocCount,
           si_file->IsCompoundFile,
           si_file->DiagnosticsCount);
    for (each = 0; each < si_file->DiagnosticsCount; each++) {
        printf("%s - %s\n",
               si_file->Diagnostics[each][0],
               si_file->Diagnostics[each][1]);
    }
    printf("SetCount: %i\n", si_file->FilesCount);
    for (each = 0; each < si_file->FilesCount; each++) {
        printf("%u - %s\n", each, si_file->Files[each]);
    }
    printf("MapCount %u\n", si_file->AttributesCount);
    for (each = 0; each < si_file->AttributesCount; each++) {
        printf("%s - %s\n",
               si_file->Attributes[each][0],
               si_file->Attributes[each][1]);
    }
    printf("numSortFields: %i\n",
           si_file->NumSortFields);

    print_codec_footer(si_file->codec_footer);
}

void free_si_file(lucene_si_file_t *si_file)
{
    unsigned int each;

    free_index_header(si_file->index_header);

    for (each = 0; each < si_file->DiagnosticsCount; each++) {
        allocation_free(si_file->Diagnostics[each][0]);
        allocation_free(si_file->Diagnostics[each][1]);
        allocation_free(si_file->Diagnostics[each]);
    }
    allocation_free(si_file->Diagnostics);

    for (each = 0; each < si_file->FilesCount; each++) {
        allocation_free(si_file->Files[each]);
    }
    allocation_free(si_file->Files);

    for (each = 0; each < si_file->AttributesCount; each++) {
        allocation_free(si_file->Attributes[each][0]);
        allocation_free(si_file->Attributes[each][1]);
        allocation_free(si_file->Attributes[each]);
    }
    allocation_free(si_file->Attributes);

    free_codec_footer(si_file->codec_footer);

    allocation_free(si_file);
}

void check_file_in_si(lucene_si_file_t *si_file, char *filename)
{
    unsigned int each;
    for (each = 0; each < si_file->FilesCount; each++) {
        if (!strcmp(filename, si_file->Files[each]))
            return;
    }
    assert(0 && "File not found!");
}
