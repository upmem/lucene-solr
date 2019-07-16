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

import com.upmem.properties.DpuType;

public final class DpuConfiguration {
  final DpuType type;
  final String profile;

  private static final String EMPTY_PROFILE = "";

  private DpuConfiguration(DpuType type, String profile) {
    this.type = type;
    this.profile = profile;
  }

  public static DpuConfiguration forSimulator(String profile) {
    return new DpuConfiguration(DpuType.simulator, profile);
  }

  public static DpuConfiguration forSimulator() {
    return DpuConfiguration.forSimulator(EMPTY_PROFILE);
  }

  public static DpuConfiguration forHardware(String profile) {
    return new DpuConfiguration(DpuType.hw, profile);
  }

  public static DpuConfiguration forHardware() {
    return DpuConfiguration.forHardware(EMPTY_PROFILE);
  }
}
