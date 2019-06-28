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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;

public final class DpuFieldReader {
  final long sumTotalTermFreq;
  final int docCount;
  final int longsSize;
  final FST<BytesRef> index;

  public DpuFieldReader(DpuIndexReader indexReader, FieldInfo fieldInfo, long numTerms, BytesRef rootCode,
                        long sumTotalTermFreq, long sumDocFreq, int docCount, long indexStartFP, int longsSize,
                        IndexInput indexIn, BytesRef minTerm, BytesRef maxTerm) throws IOException {
    this.sumTotalTermFreq = sumTotalTermFreq;
    this.docCount = docCount;
    this.longsSize = longsSize;

    if (indexIn != null) {
      final IndexInput clone = indexIn.clone();
      clone.seek(indexStartFP);
      index = new FST<>(clone, ByteSequenceOutputs.getSingleton());
    } else {
      index = null;
    }
  }
}
