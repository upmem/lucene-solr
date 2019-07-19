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
package org.apache.lucene.dpu;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import org.apache.lucene.util.IOUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertTrue;

public class TestDemo {
  private static final String dpuTarget = System.getenv("UPMEM_DPU_TARGET");
  private static final String wikipediaDatasetDirectoryName = System.getenv("UPMEM_DPU_DATASET");

  private Path javaTempDir;

  @Rule
  public TestRule folderRule = new TestRuleAdapter() {
    private FileSystem fileSystem;

    @Override
    protected void before() throws Throwable {
      super.before();

      fileSystem = FileSystems.getDefault().provider().getFileSystem(URI.create("file:///"));

      Path tmpDir = fileSystem.getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")), "DpuDemo");
      Files.createDirectories(tmpDir);

      javaTempDir = tmpDir.toRealPath();
    }

    @Override
    protected void afterAlways(List<Throwable> errors) throws Throwable {
      IOUtils.rm(javaTempDir);
    }
  };

  private void testOneSearch(Path indexPath, String query, int expectedHitCount) throws Exception {
    String target;

    if (dpuTarget == null) {
      target = "fsim";
    } else if ("fpga".equals(dpuTarget)) {
      target = "hw";
    } else {
      target = dpuTarget;
    }

    PrintStream outSave = System.out;
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      PrintStream fakeSystemOut = new PrintStream(bytes, false, Charset.defaultCharset().name());
      System.setOut(fakeSystemOut);
      SearchFiles.main(new String[] {"-query", query, "-index", indexPath.toString(), "-target", target});
      fakeSystemOut.flush();
      String output = bytes.toString(Charset.defaultCharset().name()); // intentionally use default encoding
      assertTrue("output=" + output, output.contains(expectedHitCount + " total matching documents"));
    } finally {
      System.setOut(outSave);
    }
  }

  @Test
  public void testIndexSearch() throws Exception {
    Path dir = getDataPath("test-files/docs");
    Path indexDir = javaTempDir;
    IndexFiles.main(new String[] { "-create", "-docs", dir.toString(), "-index", indexDir.toString()});
    testOneSearch(indexDir, "apache", 3);
    testOneSearch(indexDir, "patent", 8);
    testOneSearch(indexDir, "lucene", 0);
    testOneSearch(indexDir, "gnu", 6);
    testOneSearch(indexDir, "derivative", 8);
    testOneSearch(indexDir, "license", 13);
  }

  @Test
  public void testBiggerIndexSearch() throws Exception {
    Assume.assumeNotNull(wikipediaDatasetDirectoryName);
    Path largerDatasetPath = Paths.get(wikipediaDatasetDirectoryName);
    Path indexDir = javaTempDir;
    IndexFiles.main(new String[] { "-create", "-docs", largerDatasetPath.toAbsolutePath().toString(), "-index", indexDir.toString()});
    testOneSearch(indexDir, "apache", 204);
    testOneSearch(indexDir, "patent", 1194);
    testOneSearch(indexDir, "lucene", 4);
    testOneSearch(indexDir, "gnu", 352);
    testOneSearch(indexDir, "derivative", 764);
    testOneSearch(indexDir, "license", 1297);
  }

  private Path getDataPath(String name) throws IOException {
    try {
      return Paths.get(this.getClass().getResource(name).toURI());
    } catch (Exception e) {
      throw new IOException("Cannot find resource: " + name);
    }
  }
}
