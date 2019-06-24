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

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public final class DpuTermsEnum extends TermsEnum {
  private DpuManager dpuManager;
  private int fieldId;

  private Iterator<DpuManager.DpuResult> results;

  @Override
  public boolean seekExact(BytesRef target) throws IOException {
    this.results = this.dpuManager.search(this.fieldId, target).iterator();

    return next() != null;
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public void seekExact(long ord) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public BytesRef term() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public long ord() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int docFreq() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public long totalTermFreq() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
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
    assert (this.results != null) : "UPMEM calling next() before doing a search";

    throw new RuntimeException("UPMEM not implemented");
  }
}
