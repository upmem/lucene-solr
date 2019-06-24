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

  @Override
  public TermsEnum iterator() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public long size() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public long getSumDocFreq() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int getDocCount() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public boolean hasFreqs() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public boolean hasOffsets() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public boolean hasPositions() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public boolean hasPayloads() {
    throw new RuntimeException("UPMEM not implemented");
  }
}
