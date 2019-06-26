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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.upmem.dpu.host.api.DpuException;
import com.upmem.dpu.host.api.DpuProgram;
import com.upmem.dpu.host.api.DpuRank;
import com.upmem.dpujni.DpuDescription;
import com.upmem.dpujni.DpuMramTransfer;
import com.upmem.properties.DpuType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.util.BytesRef;

public final class DpuManager {
  private static final DpuType DEFAULT_DPU_TYPE = DpuType.simulator;
  private static final String DEFAULT_DPU_PROFILE = "";
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
  private static final int MAX_FIELD_SIZE = 4;
  private static final int QUERY_BUFFER_SIZE = MAX_VALUE_SIZE + MAX_FIELD_SIZE;
  private static final int SEGMENT_SUMMARY_OFFSET = DMA_ALIGNED(QUERY_BUFFER_OFFSET + QUERY_BUFFER_SIZE);
  private static final int SEGMENT_SUMMARY_ENTRY_SIZE = 8;
  private static final int SEGMENTS_OFFSET = DMA_ALIGNED(SEGMENT_SUMMARY_OFFSET + NR_THREADS * SEGMENT_SUMMARY_ENTRY_SIZE);

  private static final int DPU_CONTEXT_NR_FIELDS_OFFSET = 0;
  private static final int DPU_CONTEXT_HAS_FREQ_OFFSET = 16;
  private static final int DPU_CONTEXT_HAS_PROX_OFFSET = 17;
  private static final int DPU_CONTEXT_HAS_PAYLOADS_OFFSET = 18;
  private static final int DPU_CONTEXT_HAS_OFFSETS_OFFSET = 19;
  private static final int DPU_CONTEXT_HAS_VECTORS_OFFSET = 20;
  private static final int DPU_CONTEXT_HAS_NORMS_OFFSET = 21;
  private static final int DPU_CONTEXT_HAS_DOCVALUES_OFFSET = 22;
  private static final int DPU_CONTEXT_HAS_POINTVALUES_OFFSET = 23;
  private static final int DPU_CONTEXT_SOFT_DELETES_FIELD_OFFSET = 24;

  private static final int DPU_CONTEXT_LENGTH = 1462;

  private static final int DPU_OUTPUT_DOC_ID_OFFSET = 0;
  private static final int DPU_OUTPUT_FREQ_OFFSET = 8;
  private static final int DPU_OUTPUT_DOC_NORM_OFFSET = 12;
  private static final int DPU_OUTPUT_LENGTH = 16;

  private static final int DPU_IDF_OUTPUT_DOC_COUNT_OFFSET = 0;
  private static final int DPU_IDF_OUTPUT_DOC_FREQ_OFFSET = 4;
  private static final int DPU_IDF_OUTPUT_TOTAL_TERM_FREQ_OFFSET = 8;

  private static String dpuSearchProgramInstance = null;

  private final DpuRank[] ranks;
  private final DpuDescription description;

  private int currentRankId;
  private int currentCiId;
  private int currentDpuId;
  private int currentThreadId;
  private int currentImageOffset;
  private final byte[] memoryImage;
  private boolean indexLoaded;

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

    this.memoryImage = new byte[this.description.mramSizeInBytes];
    this.indexLoaded = false;
  }

  public void loadSegment(int segmentNumber, FieldInfos fieldInfos) throws IOException {
    assert this.currentRankId < this.ranks.length : "UPMEM too many segments for the number of allocated DPUs";

    int offsetAddress = SEGMENT_SUMMARY_OFFSET + this.currentThreadId * SEGMENT_SUMMARY_ENTRY_SIZE;
    long offset = (this.currentImageOffset & 0xffffffffL) | ((segmentNumber & 0xffffffffL) << 32);
    write(offset, this.memoryImage, offsetAddress);

    int searchContextOffset = this.currentImageOffset;
    write(fieldInfos.size(), this.memoryImage, searchContextOffset + DPU_CONTEXT_NR_FIELDS_OFFSET);
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
    // todo(UPMEM) write(nr_norms_entries, this.memoryImage, searchContextOffset + DPU_CONTEXT_NR_NORMS_ENTRIES_OFFSET);
    // todo(UPMEM) write(for_util, this.memoryImage, searchContextOffset + DPU_CONTEXT_FOR_UTIL_OFFSET);
    this.currentImageOffset = DMA_ALIGNED(this.currentImageOffset + DPU_CONTEXT_LENGTH);
    // todo(UPMEM) write norms


    // todo(UPMEM)
    // throw new RuntimeException("UPMEM not implemented");

    this.currentThreadId++;

    if (this.currentThreadId == NR_THREADS) {
      loadMemoryImage();

      this.currentThreadId = 0;
      this.currentImageOffset = 0;

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

  private static int numBytesPerValue(long min, long max) {
    if (min >= max) {
      return 0;
    } else if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
      return 1;
    } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
      return 2;
    } else if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
      return 4;
    } else {
      return 8;
    }
  }

  public void finalizeIndexLoading() {
    if (this.currentImageOffset != 0) {
      loadMemoryImage();
    }
    this.indexLoaded = true;
  }

  private void loadMemoryImage() {
    this.ranks[this.currentRankId]
        .get(this.currentCiId, this.currentDpuId)
        .copyToMram(MEMORY_IMAGE_OFFSET, this.memoryImage, this.currentImageOffset);
  }

  final static class DpuResult {
    byte[] outputs = new byte[OUTPUTS_BUFFER_SIZE];
    byte[] idfOutput = new byte[IDF_OUTPUT_SIZE];

    private int currentThreadId = 0;
    private int currentOffset = 0;
    private boolean finished = false;

    int next() {
      if (finished) {
        return -1;
      }

      int docId;

      while (true) {
        docId = readInt(outputs, currentOffset + DPU_OUTPUT_DOC_ID_OFFSET);

        if (docId != -1) {
          currentOffset += DPU_OUTPUT_LENGTH;
          break;
        }

        if (currentThreadId == (NR_THREADS - 1)) {
          finished = true;
          break;
        }

        currentThreadId++;
        currentOffset = currentThreadId * OUTPUTS_PER_THREAD;
      }

      return docId;
    }

    public int getDocFreq() {
      return readInt(idfOutput, DPU_IDF_OUTPUT_DOC_FREQ_OFFSET);
    }

    public long getTotalTermFreq() {
      return readLong(idfOutput, DPU_IDF_OUTPUT_TOTAL_TERM_FREQ_OFFSET);
    }
  }

  public List<DpuResult> search(int fieldId, BytesRef target) {
    assert this.indexLoaded : "UPMEM calling search() before loading index";

    loadQuery(fieldId, target);
    return doSearch();
  }

  private void loadQuery(int fieldId, BytesRef target) {
    assert (target.length <= MAX_VALUE_SIZE) : "UPMEM specified query value is too big";

    DpuMramTransfer transfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);

    byte[] query = new byte[QUERY_BUFFER_SIZE];
    System.arraycopy(target.bytes, target.offset, query, 0, target.length);

    write(fieldId, query, MAX_VALUE_SIZE);

    //todo(UPMEM) only load used DPUs ?
    for (int eachCi = 0; eachCi < this.description.nrOfControlInterfaces; eachCi++) {
      for (int eachDpu = 0; eachDpu < this.description.nrOfDpusPerControlInterface; eachDpu++) {
        transfer.add(eachCi, eachDpu, QUERY_BUFFER_OFFSET, query);
      }
    }

    for (DpuRank rank : this.ranks) {
      rank.copyToMrams(transfer);
    }
  }

  private List<DpuResult> doSearch() {
    List<DpuResult> results = new ArrayList<>(this.ranks.length * this.description.nrOfControlInterfaces *
        this.description.nrOfControlInterfaces);

    //todo(UPMEM) only boot used DPUs ?
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
            } else {
              stopped = false;
            }
          }
        }

        if (stopped) {
          newlyStoppedRanks.add(rank);

          DpuMramTransfer outputsTransfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);
          DpuMramTransfer idfOutputsTransfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);
          for (int eachCi = 0; eachCi < this.description.nrOfControlInterfaces; eachCi++) {
            for (int eachDpu = 0; eachDpu < this.description.nrOfDpusPerControlInterface; eachDpu++) {
              DpuResult result = new DpuResult();
              results.add(result);
              outputsTransfer.add(eachCi, eachDpu, OUTPUTS_BUFFER_OFFSET, result.outputs);
              idfOutputsTransfer.add(eachCi, eachDpu, IDF_OUTPUT_OFFSET, result.idfOutput);
            }
          }

          rank.copyFromMrams(outputsTransfer);
          rank.copyFromMrams(idfOutputsTransfer);
        } else if (faulting) {
          faultyRanks.add(rank);
          newlyStoppedRanks.add(rank);
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
