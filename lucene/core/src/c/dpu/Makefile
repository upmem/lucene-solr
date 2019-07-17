CC := clang --target=dpu-upmem-dpurte
CFLAGS := -Wall -Os -g

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MKFILE_PATH))
SRCDIR := $(BASEDIR)/src
COMDIR := $(SRCDIR)/commons
SOURCES := $(wildcard $(SRCDIR)/*.c)
SRC_HEADERS := $(wildcard $(SRCDIR)/*.h)
COMMONS_HEADERS := $(wildcard $(COMDIR)/*.h)
HEADERS := $(SRC_HEADERS) $(COMMONS_HEADERS)

ifndef BUILDDIR
BUILDDIR := .
endif
EXEC := $(BUILDDIR)/term_search.dpu

.PHONY: all clean

all: $(EXEC)

$(EXEC): $(SOURCES) $(HEADERS)
	$(CC) -o $@ $(SOURCES) $(CFLAGS) -I$(COMDIR)

clean:
	rm -f $(EXEC)
