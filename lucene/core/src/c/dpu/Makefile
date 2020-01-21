NR_TASKLETS ?= 10

CC := clang --target=dpu-upmem-dpurte
CFLAGS := -Wall -Os -g -DNR_TASKLETS=${NR_TASKLETS}

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MKFILE_PATH))
SRCDIR := $(BASEDIR)/src
SOURCES := $(wildcard $(SRCDIR)/*.c)
HEADERS := $(wildcard $(SRCDIR)/*.h)

FORMATED_FILES=${SOURCES} ${HEADERS}
CHECK_FORMAT_DEPENDENCIES=$(addsuffix -check-format,${FORMATED_FILES})
DO_FORMAT_DEPENDENCIES=$(addsuffix -do-format,${FORMATED_FILES})

ifndef BUILDDIR
BUILDDIR := .
endif
EXEC := $(BUILDDIR)/term_search.dpu

.PHONY: all clean check-format

all: $(EXEC)

$(EXEC): $(SOURCES) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(SOURCES)

clean:
	rm -f $(EXEC)

%-check-format: %
	clang-format $< | diff -y --suppress-common-lines $< -

%-do-format: %
	clang-format -i $<

check-format: ${CHECK_FORMAT_DEPENDENCIES}

fmt: ${DO_FORMAT_DEPENDENCIES}

