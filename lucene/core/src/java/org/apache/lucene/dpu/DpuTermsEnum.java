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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public final class DpuTermsEnum extends TermsEnum {
  private final DpuTerms terms;

  private List<DpuManager.DpuResult> results;
  private int resultIndex;
  private int maxResults;

  DpuTermsEnum(DpuTerms terms) {
    this.terms = terms;
  }

  @Override
  public boolean seekExact(BytesRef target) throws IOException {
    this.results = this.terms.indexReader.dpuManager.search(this.terms.fieldNumber, target);
    this.resultIndex = 0;
    this.maxResults = this.results.size();

    return goToNextResult() != -1;
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public BytesRef term() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int docFreq() throws IOException {
    return this.results.get(this.resultIndex).getDocFreq();
  }

  @Override
  public long totalTermFreq() throws IOException {
    return this.results.get(this.resultIndex).getTotalTermFreq();
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public BytesRef next() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  private int goToNextResult() {
    assert (this.results != null) : "UPMEM calling next() before doing a search";

    if (this.resultIndex == this.maxResults) {
      return -1;
    }

    int docId;

    while (true) {
      DpuManager.DpuResult result = this.results.get(this.resultIndex);
      docId = result.next();

      if (docId != -1) {
        break;
      }

      if (this.resultIndex == (this.maxResults - 1)) {
        break;
      }

      this.resultIndex++;
    }

    return docId;
  }

  @Override
  public void seekExact(long ord) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() throws IOException {
    throw new UnsupportedOperationException();
  }
}
