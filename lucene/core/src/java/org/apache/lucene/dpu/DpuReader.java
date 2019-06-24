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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.util.Bits;

public final class DpuReader extends CodecReader {
  @Override
  public StoredFieldsReader getFieldsReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public NormsProducer getNormsReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public FieldsProducer getPostingsReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public PointsReader getPointsReader() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
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
  public LeafMetaData getMetaData() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int numDocs() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public int maxDoc() {
    throw new RuntimeException("UPMEM not implemented");
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    throw new RuntimeException("UPMEM not implemented");
  }
}
