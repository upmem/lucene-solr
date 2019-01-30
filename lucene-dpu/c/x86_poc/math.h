/*
 * Copyright (c) 2014-2019 - uPmem
 */

#ifndef X86_POC_MATH_H
#define X86_POC_MATH_H

#include <math.h>

#define math_min(x, y) (((x) < (y)) ? (x) : (y))
#define math_max(x, y) (((x) > (y)) ? (x) : (y))
#define math_ceil(f) ((int) ceil(f))

#endif //X86_POC_MATH_H
