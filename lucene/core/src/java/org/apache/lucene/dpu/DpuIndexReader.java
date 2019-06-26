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
import java.util.TreeMap;

import com.upmem.dpu.host.api.DpuException;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

public final class DpuIndexReader extends LeafReader {
  private final Directory directory;
  private final SegmentInfos segmentInfos;

  final DpuManager dpuManager;
  private final TreeMap<String, DpuTerms> fields;

  private final int numDocs;
  private final int maxDoc;

  public DpuIndexReader(Directory directory, SegmentInfos sis) throws DpuException, IOException {
    this.directory = directory;
    this.segmentInfos = sis;
    this.fields = new TreeMap<>();
    this.dpuManager = new DpuManager(sis.size());

    int maxDoc = 0;
    int numDocs = 0;
    int segmentNumber = 0;
    for (SegmentCommitInfo si : sis) {
      Directory dir;

      if (si.info.getUseCompoundFile()) {
        dir = si.info.getCodec().compoundFormat().getCompoundReader(si.info.dir, si.info, IOContext.READONCE);
      } else {
        dir = si.info.dir;
      }

      FieldInfosFormat fisFormat = si.info.getCodec().fieldInfosFormat();
      FieldInfos fieldInfos = fisFormat.read(dir, si.info, "", IOContext.READONCE);

      final SegmentReadState segmentReadState = new SegmentReadState(dir, si.info, fieldInfos, IOContext.READONCE);
      FieldsProducer fieldsProducer = si.info.getCodec().postingsFormat().fieldsProducer(segmentReadState);

      this.dpuManager.loadSegment(segmentNumber, fieldInfos);

      for (FieldInfo fieldInfo : fieldInfos) {
        String fieldName = fieldInfo.name;
        Terms terms = fieldsProducer.terms(fieldName);

        if (terms == null) {
          continue;
        }

        DpuTerms dpuTerms = this.fields.get(fieldName);

        if (dpuTerms == null) {
          dpuTerms = new DpuTerms(this, fieldInfo.number);
          this.fields.put(fieldName, dpuTerms);
        }

        dpuTerms.add(fieldInfo.number, terms.getSumTotalTermFreq(), terms.getSumDocFreq(), terms.getDocCount(),
            terms.hasFreqs(), terms.hasOffsets(), terms.hasPositions(), terms.hasPayloads());
      }
      numDocs += si.info.maxDoc() - si.getDelCount();
      maxDoc += si.info.maxDoc();
      segmentNumber++;
    }
    this.numDocs = numDocs;
    this.maxDoc = maxDoc;
  }

  public static DpuIndexReader open(final Directory directory) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DpuIndexReader>(directory) {
      @Override
      protected DpuIndexReader doBody(String segmentFileName) throws IOException {
        SegmentInfos sis = SegmentInfos.readCommit(directory, segmentFileName);

        try {
          return new DpuIndexReader(directory, sis);
        } catch (DpuException e) {
          throw new IOException(e);
        }
      }
    }.run();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public Terms terms(String field) throws IOException {
    return this.fields.get(field);
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public FieldInfos getFieldInfos() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public Bits getLiveDocs() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public PointValues getPointValues(String field) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public void checkIntegrity() throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public LeafMetaData getMetaData() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int numDocs() {
    return this.numDocs;
  }

  @Override
  public int maxDoc() {
    return this.maxDoc;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  protected void doClose() throws IOException {

  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    throw new RuntimeException("UPMEM not implemented");
  }
}
