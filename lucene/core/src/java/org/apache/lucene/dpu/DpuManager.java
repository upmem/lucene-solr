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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.upmem.dpu.host.api.DpuException;
import com.upmem.dpu.host.api.DpuRank;
import com.upmem.dpujni.DpuDescription;
import com.upmem.dpujni.DpuMramTransfer;
import com.upmem.properties.DpuType;
import org.apache.lucene.util.BytesRef;

public final class DpuManager {
  private static final int SYSTEM_THREAD = 0;
  private static final int NR_THREADS = 10;
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
  private static final int SEGMENT_SUMMARY_OFFSET = DMA_ALIGNED((QUERY_BUFFER_OFFSET + QUERY_BUFFER_SIZE);
  private static final int SEGMENT_SUMMARY_ENTRY_SIZE = 8;
  private static final int SEGMENTS_OFFSET = DMA_ALIGNED(SEGMENT_SUMMARY_OFFSET + NR_THREADS * SEGMENT_SUMMARY_ENTRY_SIZE);

  private final DpuRank[] ranks;
  private final DpuDescription description;

  public DpuManager(int nrOfDpus, DpuType type, String profile) throws DpuException {
    this.description = DpuRank.getDescription(type, profile);
    int nrOfDpusPerRank = this.description.nrOfControlInterfaces * this.description.nrOfDpusPerControlInterface;
    int nrOfRanks = (nrOfDpus / nrOfDpusPerRank) + (((nrOfDpus % nrOfDpusPerRank) == 0) ? 0 : 1);

    this.ranks = new DpuRank[nrOfRanks];

    for (int rankIndex = 0; rankIndex < this.ranks.length; rankIndex++) {
      this.ranks[rankIndex] = DpuRank.allocate(type, profile);
    }
  }

  public final static class DpuResult {
    byte[] outputs = new byte[OUTPUTS_BUFFER_SIZE];
    byte[] idf_output = new byte[IDF_OUTPUT_SIZE];
  }

  public List<DpuResult> search(int fieldId, BytesRef target) {
    this.loadQuery(fieldId, target);
    return this.doSearch();
  }

  private void loadQuery(int fieldId, BytesRef target) {
    assert (target.length <= MAX_VALUE_SIZE) : "UPMEM specified query value is too big";

    DpuMramTransfer transfer = new DpuMramTransfer(this.description.nrOfControlInterfaces, this.description.nrOfDpusPerControlInterface);

    byte[] query = new byte[QUERY_BUFFER_SIZE];
    System.arraycopy(target.bytes, target.offset, query, 0, target.length);

    query[MAX_VALUE_SIZE + 0] = (byte) ((fieldId >>  0) & 0xff);
    query[MAX_VALUE_SIZE + 1] = (byte) ((fieldId >>  8) & 0xff);
    query[MAX_VALUE_SIZE + 2] = (byte) ((fieldId >> 16) & 0xff);
    query[MAX_VALUE_SIZE + 3] = (byte) ((fieldId >> 24) & 0xff);

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
              idfOutputsTransfer.add(eachCi, eachDpu, IDF_OUTPUT_OFFSET, result.idf_output);
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
}
