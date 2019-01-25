#include "parser_index_header.h"
#include "allocation.h"
#include "data_input.h"

#include <string.h>
#include <stdio.h>

lucene_index_header_t *read_index_header(data_input_t *buffer)
{
    uint32_t unused_length;
    lucene_index_header_t *index_header = allocation_get(sizeof(lucene_index_header_t));

    index_header->Magic = read_int(buffer);
    index_header->CodecName = read_string(buffer, &unused_length);
    index_header->Version = read_int(buffer);

    read_bytes(buffer, index_header->ObjectID, 0, LUCENE_INDEX_HEADER_OBJECTID_SIZE);

    index_header->SuffixLength = incremental_read_byte(buffer);
    index_header->SuffixBytes = allocation_get(index_header->SuffixLength * sizeof(uint8_t));
    read_bytes(buffer, index_header->SuffixBytes, 0, index_header->SuffixLength);

    return index_header;
}

void print_index_header(lucene_index_header_t *index_header)
{
    unsigned int each;
    printf("Magic: %u\n"
           "CodecName: %s\n"
           "Version: %u\n",
           index_header->Magic,
           index_header->CodecName,
           index_header->Version);
    printf("ObjectID: [");
    for (each = 0; each < LUCENE_INDEX_HEADER_OBJECTID_SIZE; each++) {
        printf("%i", index_header->ObjectID[each]);
        if (each != LUCENE_INDEX_HEADER_OBJECTID_SIZE - 1)
            printf(", ");
    }
    printf("]\n");
    printf("SuffixLength: %u\n", index_header->SuffixLength);
    printf("SuffixBytes: [");
    for (each = 0; each < index_header->SuffixLength; each++) {
        printf("%i", index_header->SuffixBytes[each]);
        if (each != index_header->SuffixLength - 1)
            printf(", ");
    }
    printf("]\n");
}

void free_index_header(lucene_index_header_t *index_header)
{
    allocation_free(index_header->CodecName);
    allocation_free(index_header->SuffixBytes);
    allocation_free(index_header);
}
