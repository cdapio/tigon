/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tigon.sql.internal;

import co.cask.tigon.io.Locations;
import co.cask.tigon.sql.manager.ExternalProgramExecutor;
import org.apache.twill.common.Services;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Compile to generate Stream Engine Binaries.
 */
public class CompileStreamBinaries {
  private static final Logger LOG = LoggerFactory.getLogger(CompileStreamBinaries.class);
  private final Location dir;

  public CompileStreamBinaries(Location dir) {
    this.dir = dir;
  }

  public void generateBinaries() throws Exception {
    Location rootTmpDir = Locations.getParent(Locations.getParent(dir));
    Location shell = new LocalLocationFactory(new File("/")).create("/").append("bin").append("sh");

    ExternalProgramExecutor copyGSEXIT = new ExternalProgramExecutor("Copy GSEXIT", dir, shell, "-c",
                                                                     "cp ../../bin/gsexit ./GSEXIT");
    copyGSEXIT.startAndWait();
    LOG.info("Copying GSEXIT: {}", copyGSEXIT);
    Services.getCompletionFuture(copyGSEXIT).get(20, TimeUnit.SECONDS);
    if (copyGSEXIT.getExitCode() != 0) {
      throw new RuntimeException("Stream Engine Binary Failed - GSEXIT copy failed");
    }
    ExternalProgramExecutor executorService = new ExternalProgramExecutor(
      "GENBINS", dir, shell, "-c", "../../bin/translate_fta -h localhost -c -S -M -C . packet_schema.txt *.gsql");
    LOG.info("Starting GENBINS : {}", executorService);
    executorService.startAndWait();
    Services.getCompletionFuture(executorService).get(20, TimeUnit.SECONDS);
    if (executorService.getExitCode() != 0) {
      throw new RuntimeException("Stream Engine Binary BUILD Failed");
    }
    ExternalProgramExecutor makeService = new ExternalProgramExecutor(
      "GENBINS", dir, shell, "-c", "make");
    LOG.info("Starting MAKE : {}", executorService);
    executorService.startAndWait();
    Services.getCompletionFuture(executorService).get(20, TimeUnit.SECONDS);
    if (executorService.getExitCode() != 0) {
      throw new RuntimeException("Stream Engine Binary MAKE Failed");
    }
  }
}
