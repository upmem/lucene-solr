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

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;

public final class DpuTerms extends Terms {
  final DpuIndexReader indexReader;

  final int fieldNumber;
  private long sumTotalTermFreq;
  private long sumDocFreq;
  private int docCount;

  private boolean hasFreqs;
  private boolean hasOffsets;
  private boolean hasPositions;
  private boolean hasPayloads;

  public DpuTerms(DpuIndexReader dpuIndexReader, int fieldNumber) {
    this.indexReader = dpuIndexReader;
    this.fieldNumber = fieldNumber;
    this.sumTotalTermFreq = 0;
    this.sumDocFreq = 0;
    this.docCount = 0;
    this.hasFreqs = true;
    this.hasOffsets = true;
    this.hasPositions = true;
    this.hasPayloads = false;
  }

  public void add(int fieldNumber, long sumTotalTermFreq, long sumDocFreq, int docCount,
                  boolean hasFreqs, boolean hasOffsets, boolean hasPositions, boolean hasPayloads) {
    assert this.fieldNumber == fieldNumber : "UPMEM field should have the same number in all segments";

    this.sumTotalTermFreq += sumTotalTermFreq;
    this.sumDocFreq += sumDocFreq;
    this.docCount += docCount;
    this.hasFreqs &= hasFreqs;
    this.hasOffsets &= hasOffsets;
    this.hasPositions &= hasPositions;
    this.hasPayloads |= hasPayloads;
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new DpuTermsEnum(this);
  }

  @Override
  public long size() throws IOException {
    return -1;
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    return this.sumTotalTermFreq;
  }

  @Override
  public long getSumDocFreq() throws IOException {
    return this.sumDocFreq;
  }

  @Override
  public int getDocCount() throws IOException {
    return this.docCount;
  }

  @Override
  public boolean hasFreqs() {
    return this.hasFreqs;
  }

  @Override
  public boolean hasOffsets() {
    return this.hasOffsets;
  }

  @Override
  public boolean hasPositions() {
    return this.hasPositions;
  }

  @Override
  public boolean hasPayloads() {
    return this.hasPayloads;
  }
}
