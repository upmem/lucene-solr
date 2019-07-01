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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;


public class FixedSizeMergePolicy extends MergePolicy {
  private final long sizeTargetInBytes;

  public FixedSizeMergePolicy(long sizeTargetInMB) {
    this.sizeTargetInBytes = sizeTargetInMB * 1024 * 1024;
  }

  private static class SegmentSizeAndDocs {
    private final SegmentCommitInfo segInfo;
    private final long sizeInBytes;
    private final String name;

    SegmentSizeAndDocs(SegmentCommitInfo info, final long sizeInBytes) {
      segInfo = info;
      this.name = info.info.name;
      this.sizeInBytes = sizeInBytes;
    }
  }

  private static class SegmentMergeGroup {
    private int sizeInBytes;
    private List<SegmentCommitInfo> segments;

    SegmentMergeGroup() {
      this.sizeInBytes = 0;
      this.segments = new ArrayList<>();
    }

    public void add(SegmentSizeAndDocs segment) {
      this.sizeInBytes += segment.sizeInBytes;
      this.segments.add(segment.segInfo);
    }
  }

  private List<SegmentSizeAndDocs> getSortedBySegmentSize(final SegmentInfos infos, final MergeContext mergeContext) throws IOException {
    List<SegmentSizeAndDocs> sortedBySize = new ArrayList<>();

    for (SegmentCommitInfo info : infos) {
      sortedBySize.add(new SegmentSizeAndDocs(info, size(info, mergeContext)));
    }

    sortedBySize.sort((o1, o2) -> {
      // Sort by largest size:
      int cmp = Long.compare(o2.sizeInBytes, o1.sizeInBytes);
      if (cmp == 0) {
        cmp = o1.name.compareTo(o2.name);
      }
      return cmp;

    });

    return sortedBySize;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);

    sortedInfos.removeIf(segSizeDocs -> merging.contains(segSizeDocs.segInfo) || segSizeDocs.sizeInBytes >= this.sizeTargetInBytes);

    PriorityQueue<SegmentMergeGroup> mergeGroups = new PriorityQueue<>((o1, o2) -> o2.sizeInBytes - o1.sizeInBytes);

    for (SegmentSizeAndDocs info : sortedInfos) {
      SegmentMergeGroup foundGroup = null;
      for (SegmentMergeGroup group : mergeGroups) {
        if (group.sizeInBytes + info.sizeInBytes <= this.sizeTargetInBytes) {
          foundGroup = group;
          break;
        }
      }
      if (foundGroup == null) {
        foundGroup = new SegmentMergeGroup();
      } else {
        mergeGroups.remove(foundGroup);
      }
      foundGroup.add(info);
      mergeGroups.add(foundGroup);
    }

    mergeGroups.removeIf(group -> group.segments.size() == 1);

    if (mergeGroups.size() == 0) {
      return null;
    }

    MergeSpecification specs = new MergeSpecification();

    for (SegmentMergeGroup group : mergeGroups) {
      specs.add(new OneMerge(group.segments));
    }

    return specs;
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);

    sortedInfos.removeIf(segSizeDocs -> segmentsToMerge.get(segSizeDocs.segInfo) == null || merging.contains(segSizeDocs.segInfo) || segSizeDocs.sizeInBytes >= this.sizeTargetInBytes);

    PriorityQueue<SegmentMergeGroup> mergeGroups = new PriorityQueue<>((o1, o2) -> o2.sizeInBytes - o1.sizeInBytes);

    for (SegmentSizeAndDocs info : sortedInfos) {
      SegmentMergeGroup foundGroup = null;
      for (SegmentMergeGroup group : mergeGroups) {
        if (group.sizeInBytes + info.sizeInBytes <= this.sizeTargetInBytes) {
          foundGroup = group;
          break;
        }
      }
      if (foundGroup == null) {
        foundGroup = new SegmentMergeGroup();
      } else {
        mergeGroups.remove(foundGroup);
      }
      foundGroup.add(info);
      mergeGroups.add(foundGroup);
    }

    if (mergeGroups.size() > maxSegmentCount) {
      throw new RuntimeException("UPMEM too many segments after merge");
    }

    mergeGroups.removeIf(group -> group.segments.size() == 1);

    if (mergeGroups.size() == 0) {
      return null;
    }

    MergeSpecification specs = new MergeSpecification();

    for (SegmentMergeGroup group : mergeGroups) {
      specs.add(new OneMerge(group.segments));
    }

    return specs;
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    throw new RuntimeException("UPMEM not implemented");
  }
}
