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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.upmem.dpu.host.api.DpuException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene50.ForUtil;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.codecs.lucene80.Lucene80NormsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.blocktree.BlockTreeTermsReader.VERSION_AUTO_PREFIX_TERMS_REMOVED;
import static org.apache.lucene.codecs.blocktree.BlockTreeTermsReader.VERSION_CURRENT;
import static org.apache.lucene.codecs.blocktree.BlockTreeTermsReader.VERSION_START;

public final class DpuIndexReader extends LeafReader {
  private final Directory directory;
  private final SegmentInfos segmentInfos;

  final DpuManager dpuManager;
  private final TreeMap<String, DpuTerms> fields;
  private final TreeMap<String, List<NumericDocValues>> norms;
  private final List<Integer> docBases;

  private final int numDocs;
  private final int maxDoc;
  private final List<StoredFieldsReader> storedFieldsReaders;

  public DpuIndexReader(Directory directory, SegmentInfos sis) throws DpuException, IOException {
    this.directory = directory;
    this.segmentInfos = sis;
    this.fields = new TreeMap<>();
    this.norms = new TreeMap<>();
    this.docBases = new ArrayList<>();
    this.storedFieldsReaders = new ArrayList<>();
    this.dpuManager = new DpuManager(sis.size());

    int maxDoc = 0;
    int numDocs = 0;
    int segmentNumber = 0;
    for (SegmentCommitInfo si : sis) {
      this.docBases.add(maxDoc);
      Directory dir;

      if (si.info.getUseCompoundFile()) {
        dir = si.info.getCodec().compoundFormat().getCompoundReader(si.info.dir, si.info, IOContext.DEFAULT);
      } else {
        dir = si.info.dir;
      }

      FieldInfosFormat fisFormat = si.info.getCodec().fieldInfosFormat();
      FieldInfos fieldInfos = fisFormat.read(dir, si.info, "", IOContext.DEFAULT);

      final SegmentReadState segmentReadState = new SegmentReadState(dir, si.info, fieldInfos, IOContext.DEFAULT);
      PostingsFormat postingsFormat = si.info.getCodec().postingsFormat();
      FieldsProducer fieldsProducer = postingsFormat.fieldsProducer(segmentReadState);

      this.storedFieldsReaders.add(si.info.getCodec().storedFieldsFormat().fieldsReader(dir, si.info, fieldInfos, IOContext.DEFAULT));

      Lucene80NormsProducer normsProducer = (Lucene80NormsProducer) si.info.getCodec().normsFormat().normsProducer(segmentReadState);

      SegmentReadState lucene50State = new SegmentReadState(segmentReadState, "Lucene50_0");
      Lucene50PostingsReader postingsReader = new Lucene50PostingsReader(lucene50State);

      Map<Integer, DpuFieldReader> fieldReaders = new HashMap<>();
      IndexInput termsIn = fetchFieldReaders(postingsReader, lucene50State, fieldReaders);

      this.dpuManager.loadSegment(segmentNumber, maxDoc, fieldInfos, fieldReaders, postingsReader.forUtil, termsIn,
          postingsReader.docIn, normsProducer.data, normsProducer.norms);

      for (FieldInfo fieldInfo : fieldInfos) {
        String fieldName = fieldInfo.name;
        Terms terms = fieldsProducer.terms(fieldName);

        if (terms != null) {
          DpuTerms dpuTerms = this.fields.computeIfAbsent(fieldName, k -> new DpuTerms(this, fieldInfo.number));

          dpuTerms.add(fieldInfo.number, terms.getSumTotalTermFreq(), terms.getSumDocFreq(), terms.getDocCount(),
              terms.hasFreqs(), terms.hasOffsets(), terms.hasPositions(), terms.hasPayloads());
        }

        if (normsProducer.norms.get(fieldInfo.number) != null) {
          NumericDocValues norms = normsProducer.getNorms(fieldInfo);

          List<NumericDocValues> storedNorms = this.norms.computeIfAbsent(fieldName, k -> new ArrayList<>());

          storedNorms.add(norms);
        }
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
    List<NumericDocValues> norms = this.norms.get(field);

    if ((norms == null) || (norms.size() == 0)) {
      return null;
    }

    if (norms.size() == 1) {
      return norms.get(0);
    }

    return new NumericDocValues() {
      private int nextLeaf;
      private NumericDocValues currentValues;
      private int currentDocBase;
      private int docID = -1;

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          if (currentValues == null) {
            if (nextLeaf == norms.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentDocBase = docBases.get(nextLeaf);
            currentValues = norms.get(nextLeaf);
            nextLeaf++;
            continue;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentDocBase + newDocID;
            return docID;
          }
        }
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int advance(int targetDocID) throws IOException {
        if (targetDocID <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = subIndex(targetDocID, docBases);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == norms.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentDocBase = docBases.get(readerIndex);
          currentValues = norms.get(readerIndex);
          if (currentValues == null) {
            return nextDoc();
          }
          nextLeaf = readerIndex+1;
        }
        int newDocID = currentValues.advance(targetDocID - currentDocBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentDocBase + newDocID;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int targetDocID) throws IOException {
        if (targetDocID < docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = subIndex(targetDocID, docBases);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == norms.size()) {
            throw new IllegalArgumentException("Out of range: " + targetDocID);
          }
          currentDocBase = docBases.get(readerIndex);
          currentValues = norms.get(readerIndex);
          nextLeaf = readerIndex+1;
        }
        docID = targetDocID;
        if (currentValues == null) {
          return false;
        }
        return currentValues.advanceExact(targetDocID - currentDocBase);
      }

      @Override
      public long longValue() throws IOException {
        return currentValues.longValue();
      }

      @Override
      public long cost() {
        // TODO
        return 0;
      }
    };
  }

  @Override
  public FieldInfos getFieldInfos() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public Bits getLiveDocs() {
    return null;
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
    int readerIndex = subIndex(docID, this.docBases);
    if (readerIndex == this.storedFieldsReaders.size()) {
      throw new IllegalArgumentException("Out of range: " + docID);
    }
    this.storedFieldsReaders.get(readerIndex).visitDocument(docID - this.docBases.get(readerIndex), visitor);
  }

  @Override
  protected void doClose() throws IOException {

  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    throw new RuntimeException("UPMEM not implemented");
  }

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tim";
  static final String TERMS_CODEC_NAME = "BlockTreeTermsDict";

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tip";
  static final String TERMS_INDEX_CODEC_NAME = "BlockTreeTermsIndex";

  private static int subIndex(int n, List<Integer> docBases) { // find
    // searcher/reader for doc n:
    int size = docBases.size();
    int lo = 0; // search starts array
    int hi = size - 1; // for first element less than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = docBases.get(mid);
      if (n < midValue)
        hi = mid - 1;
      else if (n > midValue)
        lo = mid + 1;
      else { // found a match
        while (mid + 1 < size && docBases.get(mid + 1) == midValue) {
          mid++; // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }

  private IndexInput fetchFieldReaders(PostingsReaderBase postingsReader, SegmentReadState state, Map<Integer, DpuFieldReader> fields) throws IOException {
    // This is almost a copy of the BlockTreeTermsReader constructor function
    boolean success = false;
    IndexInput indexIn = null;

    String segment = state.segmentInfo.name;

    String termsName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_EXTENSION);
    IndexInput termsIn;
    try {
      termsIn = state.directory.openInput(termsName, state.context);
      int version = CodecUtil.checkIndexHeader(termsIn, TERMS_CODEC_NAME, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

      if (version < VERSION_AUTO_PREFIX_TERMS_REMOVED) {
        // pre-6.2 index, records whether auto-prefix terms are enabled in the header
        byte b = termsIn.readByte();
        if (b != 0) {
          throw new CorruptIndexException("Index header pretends the index has auto-prefix terms: " + b, termsIn);
        }
      }

      String indexName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexIn = state.directory.openInput(indexName, state.context);
      CodecUtil.checkIndexHeader(indexIn, TERMS_INDEX_CODEC_NAME, version, version, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.checksumEntireFile(indexIn);

      // Have PostingsReader init itself
      postingsReader.init(termsIn, state);

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(termsIn);

      // Read per-field details
      seekDir(termsIn);
      seekDir(indexIn);

      final int numFields = termsIn.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields, termsIn);
      }

      for (int i = 0; i < numFields; ++i) {
        final int field = termsIn.readVInt();
        final long numTerms = termsIn.readVLong();
        if (numTerms <= 0) {
          throw new CorruptIndexException("Illegal numTerms for field number: " + field, termsIn);
        }
        final BytesRef rootCode = readBytesRef(termsIn);
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo == null) {
          throw new CorruptIndexException("invalid field number: " + field, termsIn);
        }
        final long sumTotalTermFreq = termsIn.readVLong();
        // when frequencies are omitted, sumDocFreq=sumTotalTermFreq and only one value is written.
        final long sumDocFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS ? sumTotalTermFreq : termsIn.readVLong();
        final int docCount = termsIn.readVInt();
        final int longsSize = termsIn.readVInt();
        if (longsSize < 0) {
          throw new CorruptIndexException("invalid longsSize for field: " + fieldInfo.name + ", longsSize=" + longsSize, termsIn);
        }
        BytesRef minTerm = readBytesRef(termsIn);
        BytesRef maxTerm = readBytesRef(termsIn);
        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
          throw new CorruptIndexException("invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(), termsIn);
        }
        if (sumDocFreq < docCount) {  // #postings must be >= #docs with field
          throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, termsIn);
        }
        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException("invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq, termsIn);
        }
        final long indexStartFP = indexIn.readVLong();
        DpuFieldReader previous = fields.put(fieldInfo.number,
            new DpuFieldReader(this, fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, docCount,
                indexStartFP, longsSize, indexIn, minTerm, maxTerm));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name, termsIn);
        }
      }

      indexIn.close();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexIn);
      }
    }

    return termsIn;
  }

  private static BytesRef readBytesRef(IndexInput in) throws IOException {
    int numBytes = in.readVInt();
    if (numBytes < 0) {
      throw new CorruptIndexException("invalid bytes length: " + numBytes, in);
    }

    BytesRef bytes = new BytesRef();
    bytes.length = numBytes;
    bytes.bytes = new byte[numBytes];
    in.readBytes(bytes.bytes, 0, numBytes);

    return bytes;
  }

  /** Seek {@code input} to the directory offset. */
  private static void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long offset = input.readLong();
    input.seek(offset);
  }
}
