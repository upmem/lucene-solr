#ifndef __NORMS_H__
#define __NORMS_H__

#include "codec_footer.h"
#include "field_info.h"
#include "mram_reader.h"
#include <norms_struct.h>
#include <stdint.h>

uint64_t getNorms(flat_norms_entry_t *entry, uint32_t doc, mram_reader_t *norms_data);

#endif /* __NORMS_H__ */
