#include <string.h>
#include <defs.h>

#include "norms.h"
#include "alloc_wrapper.h"

uint64_t getNorms(flat_norms_entry_t *entry, uint32_t doc, mram_reader_t* norms_data) {
    if (entry->docsWithFieldOffset == -2) {
        return 0;
    } else if (entry->docsWithFieldOffset == -1) {
        if (entry->bytesPerNorm == 0) {
            return entry->normsOffset;
        }
        mram_reader_t slice;
        mram_reader_fill(&slice, norms_data);
        mram_skip_bytes(&slice, entry->normsOffset, false);
        mram_skip_bytes(&slice, doc * entry->bytesPerNorm, false);
        switch (entry->bytesPerNorm) {
            case 1:
                return mram_read_byte(&slice, false);
            case 2:
                return mram_read_short(&slice, false);
            case 4:
                return mram_read_int(&slice, false);
            case 8:
                return mram_read_long(&slice, false);
            default:
                halt();
        }
    } else {
        halt();
    }
    return 0;
}
