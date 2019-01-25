#include "parser_cfe.h"
#include "allocation.h"
#include "data_input.h"

#include <stdlib.h>

lucene_cfe_file_t *parse_cfe_file(FILE *f_cfe)
{
    lucene_cfe_file_t *cfe_file;
    size_t cfe_file_size;
    data_input_t buffer =
        {
         .index = 0,
         .read_byte = incremental_read_byte,
         .skip_bytes = incremental_skip_bytes,
        };
    unsigned int each;
    uint32_t unused_length;

    fseek(f_cfe, 0, SEEK_END);
    cfe_file_size = ftell(f_cfe);
    rewind(f_cfe);

    buffer.buffer = malloc(cfe_file_size);
    fread(buffer.buffer, 1, cfe_file_size, f_cfe);

    cfe_file = allocation_get(sizeof(lucene_cfe_file_t));

    cfe_file->index_header = read_index_header(&buffer);

    cfe_file->NumEntries = read_vint(&buffer);

    cfe_file->entries = allocation_get(sizeof(lucene_cfe_entries_t) * cfe_file->NumEntries);
    for (each = 0; each < cfe_file->NumEntries; each++) {
        cfe_file->entries[each].entry_name = read_string(&buffer, &unused_length);
        cfe_file->entries[each].offset = read_long(&buffer);
        cfe_file->entries[each].length = read_long(&buffer);
    }

    cfe_file->codec_footer = read_codec_footer(&buffer);

    free(buffer.buffer);
    return cfe_file;
}

void print_cfe_file(lucene_cfe_file_t *cfe_file)
{
    unsigned int each;

    print_index_header(cfe_file->index_header);
    printf("numEntries: %i\n", cfe_file->NumEntries);
    for (each = 0; each < cfe_file->NumEntries; each++) {
        printf("%s - 0x%x - 0x%x\n",
               cfe_file->entries[each].entry_name,
               cfe_file->entries[each].offset,
               cfe_file->entries[each].length);
    }

    print_codec_footer(cfe_file->codec_footer);
}

void free_cfe_file(lucene_cfe_file_t *cfe_file)
{
    unsigned int each;
    free_index_header(cfe_file->index_header);

    for (each = 0; each < cfe_file->NumEntries; each++) {
        allocation_free(cfe_file->entries[each].entry_name);
    }
    allocation_free(cfe_file->entries);

    free_codec_footer(cfe_file->codec_footer);
    allocation_free(cfe_file);
}
