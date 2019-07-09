/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) 2014-2019 - UPMEM
 */
package org.apache.lucene.dpu;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.upmem.dpu.host.api.DpuException;
import com.upmem.dpu.host.api.DpuProgram;
import com.upmem.dpu.host.api.DpuRank;
import com.upmem.dpujni.DpuDescription;
import com.upmem.dpujni.DpuMramTransfer;
import com.upmem.properties.DpuType;
import org.apache.lucene.codecs.lucene50.ForUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.BulkOperationPacked;
import org.apache.lucene.util.packed.PackedInts;

public final class DpuManager {
  private static final DpuType DEFAULT_DPU_TYPE = DpuType.simulator;
  private static final String DEFAULT_DPU_PROFILE = "enableDbgLib=true";
  private static final String DPU_SEARCH_PROGRAM = "org/apache/lucene/dpu/term_search.dpu";
  private static final int SYSTEM_THREAD = 0;
  private static final int NR_THREADS = 10;
  private static final int MEMORY_IMAGE_OFFSET = 0;
  private static final int IDF_OUTPUT_OFFSET = 0;
  private static final int IDF_OUTPUT_SIZE = 16;
  private static final int OUTPUTS_BUFFER_OFFSET = DMA_ALIGNED(IDF_OUTPUT_OFFSET + IDF_OUTPUT_SIZE);
  private static final int OUTPUT_SIZE = 16;
  private static final int OUTPUTS_PER_THREAD = 64 * 1024;
  private static final int OUTPUTS_BUFFER_SIZE_PER_THREAD = OUTPUTS_PER_THREAD * OUTPUT_SIZE;
  private static final int OUTPUTS_BUFFER_SIZE = OUTPUTS_BUFFER_SIZE_PER_THREAD * NR_THREADS;
  private static final int QUERY_BUFFER_OFFSET = DMA_ALIGNED(OUTPUTS_BUFFER_OFFSET + OUTPUTS_BUFFER_SIZE);
  private static final int MAX_VALUE_SIZE = 20;
  private static final int MAX_FIELD_SIZE = 16;
  private static final int QUERY_BUFFER_SIZE = MAX_VALUE_SIZE + 4;
  private static final int SEGMENT_SUMMARY_OFFSET = DMA_ALIGNED(QUERY_BUFFER_OFFSET + QUERY_BUFFER_SIZE);
  private static final int SEGMENT_SUMMARY_ENTRY_SIZE = 8;
  private static final int SEGMENTS_OFFSET = DMA_ALIGNED(SEGMENT_SUMMARY_OFFSET + NR_THREADS * SEGMENT_SUMMARY_ENTRY_SIZE);

  private static final int DPU_PACKED_INT_DECODER_BITS_PER_VALUE_OFFSET = 0;
  private static final int DPU_PACKED_INT_DECODER_LONG_BLOCK_COUNT_OFFSET = DPU_PACKED_INT_DECODER_BITS_PER_VALUE_OFFSET + 4;
  private static final int DPU_PACKED_INT_DECODER_LONG_VALUE_COUNT_OFFSET = DPU_PACKED_INT_DECODER_LONG_BLOCK_COUNT_OFFSET + 4;
  private static final int DPU_PACKED_INT_DECODER_BYTE_BLOCK_COUNT_OFFSET = DPU_PACKED_INT_DECODER_LONG_VALUE_COUNT_OFFSET + 4;
  private static final int DPU_PACKED_INT_DECODER_BYTE_VALUE_COUNT_OFFSET = DPU_PACKED_INT_DECODER_BYTE_BLOCK_COUNT_OFFSET + 4;
  private static final int DPU_PACKED_INT_DECODER_MASK_OFFSET = DPU_PACKED_INT_DECODER_BYTE_VALUE_COUNT_OFFSET + 4 + /* padding */ 4;
  private static final int DPU_PACKED_INT_DECODER_INT_MASK_OFFSET = DPU_PACKED_INT_DECODER_MASK_OFFSET + 8;
  private static final int DPU_PACKED_INT_DECODER_LENGTH = DPU_PACKED_INT_DECODER_INT_MASK_OFFSET + 4 + /* padding */ 4;

  private static final int DPU_FOR_UTIL_SETUP_DONE_OFFSET = 0;
  private static final int DPU_FOR_UTIL_ENCODED_SIZES_OFFSET = DPU_FOR_UTIL_SETUP_DONE_OFFSET + 33 + /* padding */ 3;
  private static final int DPU_FOR_UTIL_DECODERS_OFFSET = DPU_FOR_UTIL_ENCODED_SIZES_OFFSET + 33 * 4;
  private static final int DPU_FOR_UTIL_ITERATIONS_OFFSET = DPU_FOR_UTIL_DECODERS_OFFSET + 33 * DPU_PACKED_INT_DECODER_LENGTH;
  private static final int DPU_FOR_UTIL_LENGTH = DPU_FOR_UTIL_ITERATIONS_OFFSET + 33 * 4 + /* padding */ 4;

  private static final int DPU_CONTEXT_NR_FIELDS_OFFSET = 0;
  private static final int DPU_CONTEXT_TERMS_IN_OFFSET = DPU_CONTEXT_NR_FIELDS_OFFSET + 4;
  private static final int DPU_CONTEXT_HAS_FREQ_OFFSET = DPU_CONTEXT_TERMS_IN_OFFSET + 12;
  private static final int DPU_CONTEXT_HAS_PROX_OFFSET = DPU_CONTEXT_HAS_FREQ_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_PAYLOADS_OFFSET = DPU_CONTEXT_HAS_PROX_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_OFFSETS_OFFSET = DPU_CONTEXT_HAS_PAYLOADS_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_VECTORS_OFFSET = DPU_CONTEXT_HAS_OFFSETS_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_NORMS_OFFSET = DPU_CONTEXT_HAS_VECTORS_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_DOCVALUES_OFFSET = DPU_CONTEXT_HAS_NORMS_OFFSET + 1;
  private static final int DPU_CONTEXT_HAS_POINTVALUES_OFFSET = DPU_CONTEXT_HAS_DOCVALUES_OFFSET + 1;
  private static final int DPU_CONTEXT_SOFT_DELETES_FIELD_OFFSET = DPU_CONTEXT_HAS_POINTVALUES_OFFSET + 1;
  private static final int DPU_CONTEXT_DOC_READER_OFFSET = DPU_CONTEXT_SOFT_DELETES_FIELD_OFFSET + MAX_FIELD_SIZE;
  private static final int DPU_CONTEXT_FOR_UTIL_OFFSET = DPU_CONTEXT_DOC_READER_OFFSET + 12 + /* padding */ 4;
  private static final int DPU_CONTEXT_NORMS_DATA_OFFSET = DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_LENGTH;
  private static final int DPU_CONTEXT_NR_NORMS_ENTRIES_OFFSET = DPU_CONTEXT_NORMS_DATA_OFFSET + 12;
  private static final int DPU_CONTEXT_EMPTY_OUTPUTS_LENGTH_OFFSET = DPU_CONTEXT_NR_NORMS_ENTRIES_OFFSET + 4;
  private static final int DPU_CONTEXT_EMPTY_OUTPUTS_OFFSET = DPU_CONTEXT_EMPTY_OUTPUTS_LENGTH_OFFSET + 4;

  private static final int DPU_FIELD_READER_NAME_OFFSET = 0;
  private static final int DPU_FIELD_READER_NUMBER_OFFSET = DPU_FIELD_READER_NAME_OFFSET + MAX_FIELD_SIZE;
  private static final int DPU_FIELD_READER_DOC_VALUES_TYPE_OFFSET = DPU_FIELD_READER_NUMBER_OFFSET + 4;
  private static final int DPU_FIELD_READER_STORE_TERM_VECTOR_OFFSET = DPU_FIELD_READER_DOC_VALUES_TYPE_OFFSET + 4;
  private static final int DPU_FIELD_READER_OMITS_NORMS_OFFSET = DPU_FIELD_READER_STORE_TERM_VECTOR_OFFSET + 1;
  private static final int DPU_FIELD_READER_INDEX_OPTIONS_OFFSET = DPU_FIELD_READER_OMITS_NORMS_OFFSET + 1 + /* padding */ 2;
  private static final int DPU_FIELD_READER_STORE_PAYLOADS_OFFSET = DPU_FIELD_READER_INDEX_OPTIONS_OFFSET + 4;
  private static final int DPU_FIELD_READER_DV_GEN_OFFSET = DPU_FIELD_READER_STORE_PAYLOADS_OFFSET + 1 + /* padding */ 7;
  private static final int DPU_FIELD_READER_POINT_DATA_DIMENSION_COUNT_OFFSET = DPU_FIELD_READER_DV_GEN_OFFSET + 8;
  private static final int DPU_FIELD_READER_POINT_INDEX_DIMENSION_COUNT_OFFSET = DPU_FIELD_READER_POINT_DATA_DIMENSION_COUNT_OFFSET + 4;
  private static final int DPU_FIELD_READER_POINT_NUM_BYTES_OFFSET = DPU_FIELD_READER_POINT_INDEX_DIMENSION_COUNT_OFFSET + 4;
  private static final int DPU_FIELD_READER_SOFT_DELETES_FIELD_OFFSET = DPU_FIELD_READER_POINT_NUM_BYTES_OFFSET + 4;
  private static final int DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET_OFFSET = DPU_FIELD_READER_SOFT_DELETES_FIELD_OFFSET + 1 + /* padding */ 3;
  private static final int DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET = DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET_OFFSET + 4;
  private static final int DPU_FIELD_READER_START_NODE_OFFSET = DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET + 4;
  private static final int DPU_FIELD_READER_INPUT_TYPE_OFFSET = DPU_FIELD_READER_START_NODE_OFFSET + 4;
  private static final int DPU_FIELD_READER_MRAM_START_OFFSET_OFFSET = DPU_FIELD_READER_INPUT_TYPE_OFFSET + 4;
  private static final int DPU_FIELD_READER_MRAM_LENGTH_OFFSET = DPU_FIELD_READER_MRAM_START_OFFSET_OFFSET + 4;
  private static final int DPU_FIELD_READER_LONGS_SIZE_OFFSET = DPU_FIELD_READER_MRAM_LENGTH_OFFSET + 4;
  private static final int DPU_FIELD_READER_DOC_COUNT_OFFSET = DPU_FIELD_READER_LONGS_SIZE_OFFSET + 4;
  private static final int DPU_FIELD_READER_SUM_TOTAL_TERM_FREQ_OFFSET = DPU_FIELD_READER_DOC_COUNT_OFFSET + 4;

  private static final int DPU_NORMS_ENTRY_DOCS_WITH_FIELD_OFFSET_OFFSET = 0;
  private static final int DPU_NORMS_ENTRY_DOCS_WITH_FIELD_LENGTH_OFFSET = DPU_NORMS_ENTRY_DOCS_WITH_FIELD_OFFSET_OFFSET + 8;
  private static final int DPU_NORMS_ENTRY_JUMP_TABLE_ENTRY_COUNT_OFFSET = DPU_NORMS_ENTRY_DOCS_WITH_FIELD_LENGTH_OFFSET + 8;
  private static final int DPU_NORMS_ENTRY_DENSE_RANK_POWER_OFFSET = DPU_NORMS_ENTRY_JUMP_TABLE_ENTRY_COUNT_OFFSET + 2;
  private static final int DPU_NORMS_ENTRY_NUM_DOCS_WITH_FIELD_OFFSET = DPU_NORMS_ENTRY_DENSE_RANK_POWER_OFFSET + 1 + /* padding */ 1;
  private static final int DPU_NORMS_ENTRY_BYTES_PER_NORM_OFFSET = DPU_NORMS_ENTRY_NUM_DOCS_WITH_FIELD_OFFSET + 4;
  private static final int DPU_NORMS_ENTRY_NORMS_OFFSET_OFFSET = DPU_NORMS_ENTRY_BYTES_PER_NORM_OFFSET + 1 + /* padding*/ 7;

  private static final int DPU_CONTEXT_LENGTH = DPU_CONTEXT_EMPTY_OUTPUTS_OFFSET + 4;
  private static final int DPU_NORMS_ENTRY_LENGTH = DPU_NORMS_ENTRY_NORMS_OFFSET_OFFSET + 8;
  private static final int DPU_FIELD_READER_LENGTH = DPU_FIELD_READER_SUM_TOTAL_TERM_FREQ_OFFSET + 8;

  private static final int DPU_OUTPUT_DOC_ID_OFFSET = 0;
  private static final int DPU_OUTPUT_FREQ_OFFSET = 8;
  private static final int DPU_OUTPUT_DOC_NORM_OFFSET = 12;
  private static final int DPU_OUTPUT_LENGTH = 16;

  private static final int DPU_IDF_OUTPUT_DOC_COUNT_OFFSET = 0;
  private static final int DPU_IDF_OUTPUT_DOC_FREQ_OFFSET = 4;
  private static final int DPU_IDF_OUTPUT_TOTAL_TERM_FREQ_OFFSET = 8;

  private static final int DPU_MRAM_READER_INDEX_OFFSET = 0;
  private static final int DPU_MRAM_READER_BASE_OFFSET = 4;
  private static final int DPU_MRAM_READER_CACHE_OFFSET = 8;


  private static final int DPU_NULL = 0;

  private static String dpuSearchProgramInstance = null;

  private final DpuRank[] ranks;
  private final DpuDescription description;

  private final Map<DpuRank, int[][][]> docBases;

  private final Map<Integer, Map<BytesRef, DpuResults>> cachedResults;

  private int currentRankId;
  private int currentCiId;
  private int currentDpuId;
  private int currentThreadId;
  private int currentImageOffset;
  private final byte[] memoryImage;
  private boolean indexLoaded;

  private final Map<Integer, Integer> fieldIdMapping;

  public DpuManager(int nrOfSegments) throws DpuException, IOException {
    this(nrOfSegments, DEFAULT_DPU_TYPE, DEFAULT_DPU_PROFILE);
  }

  public DpuManager(int nrOfSegments, DpuType type, String profile) throws DpuException, IOException {
    int nrOfDpus = (nrOfSegments / NR_THREADS) + (((nrOfSegments % NR_THREADS) == 0) ? 0 : 1);
    this.description = DpuRank.getDescription(type, profile);
    int nrOfDpusPerRank = this.description.nrOfControlInterfaces * this.description.nrOfDpusPerControlInterface;
    int nrOfRanks = (nrOfDpus / nrOfDpusPerRank) + (((nrOfDpus % nrOfDpusPerRank) == 0) ? 0 : 1);

    this.ranks = new DpuRank[nrOfRanks];

    String executableFileName = fetchDpuSearchProgram();
    DpuProgram program = DpuProgram.from(executableFileName);

    for (int rankIndex = 0; rankIndex < this.ranks.length; rankIndex++) {
      this.ranks[rankIndex] = DpuRank.allocate(type, profile);
      this.ranks[rankIndex].load(program);
    }

    this.docBases = new HashMap<>();
    this.cachedResults = new HashMap<>();

    this.memoryImage = new byte[this.description.mramSizeInBytes];
    this.currentImageOffset = SEGMENTS_OFFSET;
    this.indexLoaded = false;

    this.fieldIdMapping = new HashMap<>();
  }

  public void loadSegment(int segmentNumber, int docBase, FieldInfos fieldInfos, Map<Integer, DpuFieldReader> fieldReaders,
                          ForUtil forUtil, IndexInput termsIn, IndexInput docIn, IndexInput normsData,
                          Map<Integer, DpuIndexReader.NormsEntry> norms) throws IOException {
    assert this.currentRankId < this.ranks.length : "UPMEM too many segments for the number of allocated DPUs";

    int[][][] rankDocBases = this.docBases.computeIfAbsent(this.ranks[this.currentRankId], k ->
        new int[this.description.nrOfControlInterfaces][this.description.nrOfDpusPerControlInterface][NR_THREADS]);

    rankDocBases[this.currentCiId][this.currentDpuId][this.currentThreadId] = docBase;

    int offsetAddress = SEGMENT_SUMMARY_OFFSET + this.currentThreadId * SEGMENT_SUMMARY_ENTRY_SIZE;
    long offset = (this.currentImageOffset & 0xffffffffL) | ((segmentNumber & 0xffffffffL) << 32);
    write(offset, this.memoryImage, offsetAddress);

    int searchContextOffset = this.currentImageOffset;
    write(fieldReaders.size(), this.memoryImage, searchContextOffset + DPU_CONTEXT_NR_FIELDS_OFFSET);
    write(fieldInfos.hasFreq(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_FREQ_OFFSET);
    write(fieldInfos.hasProx(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_PROX_OFFSET);
    write(fieldInfos.hasPayloads(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_PAYLOADS_OFFSET);
    write(fieldInfos.hasOffsets(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_OFFSETS_OFFSET);
    write(fieldInfos.hasVectors(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_VECTORS_OFFSET);
    write(fieldInfos.hasNorms(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_NORMS_OFFSET);
    write(fieldInfos.hasDocValues(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_DOCVALUES_OFFSET);
    write(fieldInfos.hasPointValues(), this.memoryImage, searchContextOffset + DPU_CONTEXT_HAS_POINTVALUES_OFFSET);
    String softDeletesField = fieldInfos.getSoftDeletesField();
    if (softDeletesField == null) {
      softDeletesField = "";
    }
    write(softDeletesField, this.memoryImage, searchContextOffset + DPU_CONTEXT_SOFT_DELETES_FIELD_OFFSET);
    int maxFieldId = -1;
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.number > maxFieldId) {
        maxFieldId = fieldInfo.number;
      }
    }
    write(maxFieldId + 1, this.memoryImage, searchContextOffset + DPU_CONTEXT_NR_NORMS_ENTRIES_OFFSET);
    PackedInts.Decoder[] decoders = forUtil.decoders;
    for (int index = 1; index < decoders.length; index++) {
      PackedInts.Decoder decoder = decoders[index];
      boolean setupDone = false;
      if (decoder instanceof BulkOperationPacked) {
        BulkOperationPacked packed = (BulkOperationPacked) decoder;
        write(packed.bitsPerValue, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_BITS_PER_VALUE_OFFSET);
        write(packed.longBlockCount(), this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_LONG_BLOCK_COUNT_OFFSET);
        write(packed.longValueCount(), this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_LONG_VALUE_COUNT_OFFSET);
        write(packed.byteBlockCount(), this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_BYTE_BLOCK_COUNT_OFFSET);
        write(packed.byteValueCount(), this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_BYTE_VALUE_COUNT_OFFSET);
        write(packed.mask, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_MASK_OFFSET);
        write(packed.intMask, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_DECODERS_OFFSET
            + index * DPU_PACKED_INT_DECODER_LENGTH + DPU_PACKED_INT_DECODER_INT_MASK_OFFSET);
        setupDone = true;
      }
      write(setupDone, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_SETUP_DONE_OFFSET + index);
    }
    write(forUtil.encodedSizes, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_ENCODED_SIZES_OFFSET);
    write(forUtil.iterations, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET + DPU_FOR_UTIL_ITERATIONS_OFFSET);
    this.currentImageOffset = DMA_ALIGNED(this.currentImageOffset + DPU_CONTEXT_LENGTH);
    for (int eachFieldId = 0; eachFieldId < maxFieldId + 1; eachFieldId++) {
      DpuIndexReader.NormsEntry normsEntry = norms.get(eachFieldId);

      if (normsEntry != null) {
        write(normsEntry.docsWithFieldOffset, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_DOCS_WITH_FIELD_OFFSET_OFFSET);
        write(normsEntry.docsWithFieldLength, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_DOCS_WITH_FIELD_LENGTH_OFFSET);
        write(normsEntry.jumpTableEntryCount, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_JUMP_TABLE_ENTRY_COUNT_OFFSET);
        write(normsEntry.numDocsWithField, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_NUM_DOCS_WITH_FIELD_OFFSET);
        write(normsEntry.bytesPerNorm, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_BYTES_PER_NORM_OFFSET);
        write(normsEntry.normsOffset, this.memoryImage, this.currentImageOffset + DPU_NORMS_ENTRY_NORMS_OFFSET_OFFSET);
      }

      this.currentImageOffset += DMA_ALIGNED(DPU_NORMS_ENTRY_LENGTH);
    }
    int nrFields = 0;
    for (FieldInfo fieldInfo : fieldInfos) {
      this.fieldIdMapping.put(fieldInfo.number, nrFields);
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        continue;
      }
      nrFields++;
    }
    int emptyOutputsOffsetStart = this.currentImageOffset + nrFields * DMA_ALIGNED(DPU_FIELD_READER_LENGTH);
    int emptyOutputsOffset = emptyOutputsOffsetStart;
    int emptyOutputsLength = 0;
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        continue;
      }

      DpuFieldReader fieldReader = fieldReaders.get(fieldInfo.number);
      emptyOutputsLength += 4 + ((fieldReader.index.getEmptyOutput().length + 3) & ~3);
    }
    write(emptyOutputsLength, this.memoryImage, searchContextOffset + DPU_CONTEXT_EMPTY_OUTPUTS_LENGTH_OFFSET);
    int fstContentsOffset = emptyOutputsOffset + emptyOutputsLength;

    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        continue;
      }

      DpuFieldReader fieldReader = fieldReaders.get(fieldInfo.number);
      write(fieldReader.longsSize, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_LONGS_SIZE_OFFSET);
      write(fieldReader.docCount, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_DOC_COUNT_OFFSET);
      write(fieldReader.sumTotalTermFreq, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_SUM_TOTAL_TERM_FREQ_OFFSET);
      write(emptyOutputsOffset - emptyOutputsOffsetStart, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET_OFFSET);
      write(0, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_EMPTY_OUTPUT_OFFSET);
      write(fieldReader.index.startNode, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_START_NODE_OFFSET);
      int inputType = 0;
      switch (fieldReader.index.inputType) {
        case BYTE1:
          inputType = 0;
          break;
        case BYTE2:
          inputType = 1;
          break;
        case BYTE4:
          inputType = 2;
          break;
      }
      write(inputType, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_INPUT_TYPE_OFFSET);
      write(fstContentsOffset, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_MRAM_START_OFFSET_OFFSET);
      write(fieldReader.index.bytesArray.length, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_MRAM_LENGTH_OFFSET);
      write(fieldInfo.name, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_NAME_OFFSET);
      write(fieldInfo.number, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_NUMBER_OFFSET);
      int docValuesType = 0;
      switch (fieldInfo.getDocValuesType()) {
        case NONE:
          docValuesType = 0;
          break;
        case NUMERIC:
          docValuesType = 1;
          break;
        case BINARY:
          docValuesType = 2;
          break;
        case SORTED:
          docValuesType = 3;
          break;
        case SORTED_NUMERIC:
          docValuesType = 4;
          break;
        case SORTED_SET:
          docValuesType = 5;
          break;
      }
      write(docValuesType, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_DOC_VALUES_TYPE_OFFSET);
      write(fieldInfo.hasVectors(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_STORE_TERM_VECTOR_OFFSET);
      write(fieldInfo.omitsNorms(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_OMITS_NORMS_OFFSET);
      int indexOptions = 0;
      switch (fieldInfo.getIndexOptions()) {
        case NONE:
          indexOptions = 0;
          break;
        case DOCS:
          indexOptions = 1;
          break;
        case DOCS_AND_FREQS:
          indexOptions = 2;
          break;
        case DOCS_AND_FREQS_AND_POSITIONS:
          indexOptions = 3;
          break;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
          indexOptions = 4;
          break;
      }
      write(indexOptions, this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_INDEX_OPTIONS_OFFSET);
      write(fieldInfo.hasPayloads(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_STORE_PAYLOADS_OFFSET);
      write(fieldInfo.getDocValuesGen(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_DV_GEN_OFFSET);
      write(fieldInfo.getPointDataDimensionCount(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_POINT_DATA_DIMENSION_COUNT_OFFSET);
      write(fieldInfo.getPointIndexDimensionCount(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_POINT_INDEX_DIMENSION_COUNT_OFFSET);
      write(fieldInfo.getPointNumBytes(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_POINT_NUM_BYTES_OFFSET);
      write(fieldInfo.isSoftDeletesField(), this.memoryImage, this.currentImageOffset + DPU_FIELD_READER_SOFT_DELETES_FIELD_OFFSET);
      write(fieldReader.index.bytesArray, this.memoryImage, fstContentsOffset);
      fstContentsOffset += fieldReader.index.bytesArray.length;
      write(fieldReader.index.getEmptyOutput().length, this.memoryImage, emptyOutputsOffset);
      write(fieldReader.index.getEmptyOutput().bytes, this.memoryImage, emptyOutputsOffset + 4);
      emptyOutputsOffset += 4 + ((fieldReader.index.getEmptyOutput().length + 3) & ~3);

      this.currentImageOffset = DMA_ALIGNED(this.currentImageOffset + DPU_FIELD_READER_LENGTH);
    }

    this.currentImageOffset = DMA_ALIGNED(fstContentsOffset);
    write(DPU_NULL, this.memoryImage, searchContextOffset + DPU_CONTEXT_EMPTY_OUTPUTS_OFFSET);
    
    int termsInLength = (int) termsIn.length();
    byte[] termsInBuffer = new byte[termsInLength];
    long termsInPos = termsIn.getFilePointer();
    termsIn.seek(0);
    termsIn.readBytes(termsInBuffer, 0, termsInLength);
    termsIn.seek(termsInPos);
    write(termsInBuffer, this.memoryImage, this.currentImageOffset);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_TERMS_IN_OFFSET + DPU_MRAM_READER_INDEX_OFFSET);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_TERMS_IN_OFFSET + DPU_MRAM_READER_BASE_OFFSET);
    write(DPU_NULL, this.memoryImage, searchContextOffset + DPU_CONTEXT_TERMS_IN_OFFSET + DPU_MRAM_READER_CACHE_OFFSET);
    this.currentImageOffset += DMA_ALIGNED(termsInLength);
    
    int docInLength = (int) docIn.length();
    byte[] docInBuffer = new byte[docInLength];
    long docInPos = docIn.getFilePointer();
    docIn.seek(0);
    docIn.readBytes(docInBuffer, 0, docInLength);
    docIn.seek(docInPos);
    write(docInBuffer, this.memoryImage, this.currentImageOffset);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_DOC_READER_OFFSET + DPU_MRAM_READER_INDEX_OFFSET);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_DOC_READER_OFFSET + DPU_MRAM_READER_BASE_OFFSET);
    write(DPU_NULL, this.memoryImage, searchContextOffset + DPU_CONTEXT_DOC_READER_OFFSET + DPU_MRAM_READER_CACHE_OFFSET);
    this.currentImageOffset += DMA_ALIGNED(docInLength);

    int normsDataLength = (int) normsData.length();
    byte[] normsDataBuffer = new byte[normsDataLength];
    long normsDataPos = normsData.getFilePointer();
    normsData.seek(0);
    normsData.readBytes(normsDataBuffer, 0, normsDataLength);
    normsData.seek(normsDataPos);
    write(normsDataBuffer, this.memoryImage, this.currentImageOffset);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_NORMS_DATA_OFFSET + DPU_MRAM_READER_INDEX_OFFSET);
    write(this.currentImageOffset, this.memoryImage, searchContextOffset + DPU_CONTEXT_NORMS_DATA_OFFSET + DPU_MRAM_READER_BASE_OFFSET);
    write(DPU_NULL, this.memoryImage, searchContextOffset + DPU_CONTEXT_NORMS_DATA_OFFSET + DPU_MRAM_READER_CACHE_OFFSET);
    this.currentImageOffset += DMA_ALIGNED(normsDataLength);

    this.currentThreadId++;

    if (this.currentThreadId == NR_THREADS) {
      loadMemoryImage();
      resetMemoryImageContent();

      this.currentThreadId = 0;
      this.currentImageOffset = SEGMENTS_OFFSET;

      if (this.currentDpuId == (this.description.nrOfDpusPerControlInterface - 1)) {
        this.currentDpuId = 0;

        if (this.currentCiId == (this.description.nrOfControlInterfaces - 1)) {
          this.currentCiId = 0;
          this.currentRankId++;
        } else {
          this.currentCiId++;
        }
      } else {
        this.currentDpuId++;
      }
    }
  }

  private void finalizeIndexLoading() throws IOException {
    if (this.currentThreadId != 0) {
      for (int eachThread = this.currentThreadId; eachThread < NR_THREADS; eachThread++) {
        int offsetAddress = SEGMENT_SUMMARY_OFFSET + eachThread * SEGMENT_SUMMARY_ENTRY_SIZE;
        long offset = 0xffffffffL | ((eachThread & 0xffffffffL) << 32);
        write(offset, this.memoryImage, offsetAddress);
      }
      loadMemoryImage();
      resetMemoryImageContent();

      this.currentThreadId = 0;
      this.currentImageOffset = SEGMENTS_OFFSET;

      if (this.currentDpuId == (this.description.nrOfDpusPerControlInterface - 1)) {
        this.currentDpuId = 0;

        if (this.currentCiId == (this.description.nrOfControlInterfaces - 1)) {
          this.currentCiId = 0;
          this.currentRankId++;
        } else {
          this.currentCiId++;
        }
      } else {
        this.currentDpuId++;
      }
    }

    if (this.currentRankId < this.ranks.length) {
      for (int eachThread = 0; eachThread < NR_THREADS; eachThread++) {
        int offsetAddress = SEGMENT_SUMMARY_OFFSET + eachThread * SEGMENT_SUMMARY_ENTRY_SIZE;
        long offset = 0xffffffffL | ((eachThread & 0xffffffffL) << 32);
        write(offset, this.memoryImage, offsetAddress);
      }

      for (; this.currentDpuId < this.description.nrOfDpusPerControlInterface; this.currentDpuId++) {
        loadMemoryImage();
      }

      for (; this.currentCiId < this.description.nrOfControlInterfaces; this.currentCiId++) {
        this.currentDpuId = 0;

        for (; this.currentDpuId < this.description.nrOfDpusPerControlInterface; this.currentDpuId++) {
          loadMemoryImage();
        }
      }
    }

    this.indexLoaded = true;
  }

  private void loadMemoryImage() throws IOException {
    Files.write(Paths.get("mram_" + this.currentRankId + "_" + this.currentCiId + "_" + this.currentDpuId + ".bin"),
        this.memoryImage);

    this.ranks[this.currentRankId]
        .get(this.currentCiId, this.currentDpuId)
        .copyToMram(MEMORY_IMAGE_OFFSET, this.memoryImage, this.currentImageOffset);
  }

  private void resetMemoryImageContent() {
    for (int eachByte = 0; eachByte < this.memoryImage.length; eachByte++) {
      this.memoryImage[eachByte] = 0;
    }
  }

  final static class DpuResults {
    int docFreq = 0;
    long totalTermFreq = 0;

    TreeSet<DpuDocResult> results = new TreeSet<>(Comparator.comparingInt(o -> o.docId));
  }

  final static class DpuDocResult {
    int docId;
    int freq;
    int docNorm;

    public DpuDocResult(int docId, int freq, int docNorm) {
      this.docId = docId;
      this.freq = freq;
      this.docNorm = docNorm;
    }
  }

  final static class RawDpuResult {
    int ciId;
    int dpuId;

    byte[] outputs = new byte[OUTPUTS_BUFFER_SIZE];
    byte[] idfOutput = new byte[IDF_OUTPUT_SIZE];

    public RawDpuResult(int ciId, int dpuId) {
      this.ciId = ciId;
      this.dpuId = dpuId;
    }
  }

  public DpuResults search(int fieldId, BytesRef target) throws IOException {
    if (!this.indexLoaded) {
      finalizeIndexLoading();
    }
    Map<BytesRef, DpuResults> resultsForTarget = this.cachedResults.computeIfAbsent(fieldId, k -> new HashMap<>());

    DpuResults results = resultsForTarget.get(target);

    if (results == null) {
      fieldId = this.fieldIdMapping.get(fieldId);
      loadQuery(fieldId, target);
      results = doSearch();
      resultsForTarget.put(target, results);
    }

    return results;
  }

  private void loadQuery(int fieldId, BytesRef target) {
    assert (target.length <= MAX_VALUE_SIZE) : "UPMEM specified query value is too big";

    DpuMramTransfer transfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);

    byte[] query = new byte[QUERY_BUFFER_SIZE];
    System.arraycopy(target.bytes, target.offset, query, 0, target.length);

    write(fieldId, query, MAX_VALUE_SIZE);

    for (int eachCi = 0; eachCi < this.description.nrOfControlInterfaces; eachCi++) {
      for (int eachDpu = 0; eachDpu < this.description.nrOfDpusPerControlInterface; eachDpu++) {
        transfer.add(eachCi, eachDpu, QUERY_BUFFER_OFFSET, query);
      }
    }

    for (DpuRank rank : this.ranks) {
      rank.copyToMrams(transfer);
    }
  }

  private DpuResults doSearch() {
    DpuResults results = new DpuResults();
    List<RawDpuResult> resultList = new ArrayList<>(this.description.nrOfControlInterfaces * this.description.nrOfControlInterfaces);

    for (DpuRank rank : this.ranks) {
      rank.launch(SYSTEM_THREAD);
    }

    int[] runBitfield = new int[this.description.nrOfControlInterfaces];
    int[] faultBitfield = new int[this.description.nrOfControlInterfaces];

    List<DpuRank> runningRanks = new ArrayList<>(this.ranks.length);
    runningRanks.addAll(Arrays.asList(this.ranks));
    Set<DpuRank> faultyRanks = new HashSet<>(this.ranks.length);
    Set<DpuRank> newlyStoppedRanks = new HashSet<>(this.ranks.length);

    while (!runningRanks.isEmpty()) {
      for (DpuRank rank : runningRanks) {
        rank.poll(runBitfield, faultBitfield);

        boolean stopped = true;
        boolean faulting = false;

        for (int eachCi = 0; eachCi < this.description.nrOfControlInterfaces; eachCi++) {
          if (runBitfield[eachCi] != 0) {
            if (faultBitfield[eachCi] != 0) {
              faulting = true;

              for (int eachDpu = 0; eachDpu < this.description.nrOfDpusPerControlInterface; eachDpu++) {
                if ((faultBitfield[eachCi] & (1 << eachDpu)) != 0) {
                  System.out.println(rank.get(eachCi, eachDpu).coreDump().report());
                }
              }
            } else {
              stopped = false;
            }
          }
        }

        if (faulting) {
          faultyRanks.add(rank);
          newlyStoppedRanks.add(rank);
        } else if (stopped) {
          newlyStoppedRanks.add(rank);

          DpuMramTransfer outputsTransfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);
          DpuMramTransfer idfOutputsTransfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);
          for (int eachCi = 0; eachCi < this.description.nrOfControlInterfaces; eachCi++) {
            for (int eachDpu = 0; eachDpu < this.description.nrOfDpusPerControlInterface; eachDpu++) {
              RawDpuResult result = new RawDpuResult(eachCi, eachDpu);
              resultList.add(result);
              outputsTransfer.add(eachCi, eachDpu, OUTPUTS_BUFFER_OFFSET, result.outputs);
              idfOutputsTransfer.add(eachCi, eachDpu, IDF_OUTPUT_OFFSET, result.idfOutput);
            }
          }

          rank.copyFromMrams(outputsTransfer);
          rank.copyFromMrams(idfOutputsTransfer);

          for (RawDpuResult result : resultList) {
            results.docFreq += readInt(result.idfOutput, DPU_IDF_OUTPUT_DOC_FREQ_OFFSET);
            results.totalTermFreq += readLong(result.idfOutput, DPU_IDF_OUTPUT_TOTAL_TERM_FREQ_OFFSET);

            boolean finished = false;
            int currentOffset = 0;
            int currentThreadId = 0;
            do {
              int docId = readInt(result.outputs, currentOffset + DPU_OUTPUT_DOC_ID_OFFSET);

              if (docId == -1) {
                if (currentThreadId == (NR_THREADS - 1)) {
                  finished = true;
                }

                currentThreadId++;
                currentOffset = currentThreadId * OUTPUTS_PER_THREAD * DPU_OUTPUT_LENGTH;
              } else {
                int freq = readInt(result.outputs, currentOffset + DPU_OUTPUT_FREQ_OFFSET);
                int docNorm = readInt(result.outputs, currentOffset + DPU_OUTPUT_DOC_NORM_OFFSET);

                int docBase = this.docBases.get(rank)[result.ciId][result.dpuId][currentThreadId];
                results.results.add(new DpuDocResult(docBase + docId, freq, docNorm));
                currentOffset += DPU_OUTPUT_LENGTH;
              }
            } while (!finished);
          }

          resultList.clear();
        }
      }

      runningRanks.removeAll(newlyStoppedRanks);
      newlyStoppedRanks.clear();
    }

    assert faultyRanks.isEmpty() : "UPMEM DPU execution fault";

    return results;
  }

  private static int DMA_ALIGNED(int x) {
    return (x + 7) & ~7;
  }

  private static long readLong(byte[] buffer, int offset) {
    byte b0 = buffer[offset + 0];
    byte b1 = buffer[offset + 1];
    byte b2 = buffer[offset + 2];
    byte b3 = buffer[offset + 3];
    byte b4 = buffer[offset + 4];
    byte b5 = buffer[offset + 5];
    byte b6 = buffer[offset + 6];
    byte b7 = buffer[offset + 7];

    return ((b0 & 0xffL) <<  0)
         | ((b1 & 0xffL) <<  8)
         | ((b2 & 0xffL) << 16)
         | ((b3 & 0xffL) << 24)
         | ((b4 & 0xffL) << 32)
         | ((b5 & 0xffL) << 40)
         | ((b6 & 0xffL) << 48)
         | ((b7 & 0xffL) << 56);
  }

  private static int readInt(byte[] buffer, int offset) {
    byte b0 = buffer[offset + 0];
    byte b1 = buffer[offset + 1];
    byte b2 = buffer[offset + 2];
    byte b3 = buffer[offset + 3];

    return ((b0 & 0xff) <<  0)
         | ((b1 & 0xff) <<  8)
         | ((b2 & 0xff) << 16)
         | ((b3 & 0xff) << 24);
  }

  private static void write(String data, byte[] buffer, int offset) {
    byte[] bytes = data.getBytes(Charset.defaultCharset());
    System.arraycopy(bytes, 0, buffer, offset, bytes.length);
    buffer[offset + bytes.length] = '\0';
  }

  private static void write(byte[] data, byte[] buffer, int offset) {
    System.arraycopy(data, 0, buffer, offset, data.length);
  }

  private static void write(int[] data, byte[] buffer, int offset) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    intBuffer.put(data);
    byte[] bytes = byteBuffer.array();
    System.arraycopy(bytes, 0, buffer, offset, bytes.length);
  }

  private static void write(long data, byte[] buffer, int offset) {
    buffer[offset + 0] = (byte) ((data >>  0) & 0xffL);
    buffer[offset + 1] = (byte) ((data >>  8) & 0xffL);
    buffer[offset + 2] = (byte) ((data >> 16) & 0xffL);
    buffer[offset + 3] = (byte) ((data >> 24) & 0xffL);
    buffer[offset + 4] = (byte) ((data >> 32) & 0xffL);
    buffer[offset + 5] = (byte) ((data >> 40) & 0xffL);
    buffer[offset + 6] = (byte) ((data >> 48) & 0xffL);
    buffer[offset + 7] = (byte) ((data >> 56) & 0xffL);
  }

  private static void write(int data, byte[] buffer, int offset) {
    buffer[offset + 0] = (byte) ((data >>  0) & 0xff);
    buffer[offset + 1] = (byte) ((data >>  8) & 0xff);
    buffer[offset + 2] = (byte) ((data >> 16) & 0xff);
    buffer[offset + 3] = (byte) ((data >> 24) & 0xff);
  }

  private static void write(short data, byte[] buffer, int offset) {
    buffer[offset + 0] = (byte) ((data >>  0) & 0xff);
    buffer[offset + 1] = (byte) ((data >>  8) & 0xff);
  }

  private static void write(byte data, byte[] buffer, int offset) {
    buffer[offset] = data;
  }

  private static void write(boolean data, byte[] buffer, int offset) {
    buffer[offset] = (byte) (data ? 1 : 0);
  }

  private synchronized static String fetchDpuSearchProgram() throws IOException {
    if (dpuSearchProgramInstance == null) {
      InputStream is = getResourceAsStream(DPU_SEARCH_PROGRAM);
      Path path = Files.createTempFile("term_search", ".dpu");
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
      path.toFile().deleteOnExit();
      dpuSearchProgramInstance = path.toAbsolutePath().toString();
    }

    return dpuSearchProgramInstance;
  }

  private static InputStream getResourceAsStream(String resource) {
    final InputStream in = getContextClassLoader().getResourceAsStream(resource);

    return in == null ? DpuManager.class.getResourceAsStream(resource) : in;
  }

  private static ClassLoader getContextClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }
}
