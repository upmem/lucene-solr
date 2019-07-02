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

import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public final class DpuTermsEnum extends TermsEnum {
  private final DpuTerms terms;

  private DpuManager.DpuResults results;

  DpuTermsEnum(DpuTerms terms) {
    this.terms = terms;
  }

  @Override
  public boolean seekExact(BytesRef target) throws IOException {
    this.results = this.terms.indexReader.dpuManager.search(this.terms.fieldNumber, target);
    return this.results.docFreq != 0;
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
    return this.results.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    return this.results.totalTermFreq;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
      throw new RuntimeException("UPMEM not implemented");
    }

    return new ImpactsEnum() {
      private int docId = -1;
      private int freq;

      private Iterator<DpuManager.DpuDocResult> iterator = DpuTermsEnum.this.results.results.iterator();

      @Override
      public void advanceShallow(int target) throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public Impacts getImpacts() throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public int freq() throws IOException {
        return this.freq;
      }

      @Override
      public int nextPosition() throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public int startOffset() throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public int endOffset() throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public BytesRef getPayload() throws IOException {
        throw new RuntimeException("UPMEM not implemented");
      }

      @Override
      public int docID() {
        return this.docId;
      }

      @Override
      public int nextDoc() throws IOException {
        int nextDoc;
        if (this.iterator.hasNext()) {
          DpuManager.DpuDocResult docResult = this.iterator.next();
          nextDoc = docResult.docId;
          this.freq = docResult.freq;
        } else {
          nextDoc = NO_MORE_DOCS;
        }
        this.docId = nextDoc;
        return nextDoc;
      }

      @Override
      public int advance(int target) throws IOException {
        int doc;
        while ((doc = nextDoc()) < target) {}
        return doc;
      }

      @Override
      public long cost() {
        throw new RuntimeException("UPMEM not implemented");
      }
    };
  }

  @Override
  public BytesRef next() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
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
