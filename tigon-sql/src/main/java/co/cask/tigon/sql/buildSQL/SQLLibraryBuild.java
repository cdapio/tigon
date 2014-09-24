/**
 * Copyright 2012-2014 Continuuity, Inc.
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


package co.cask.tigon.sql.buildsql;

import co.cask.tigon.sql.util.Platform;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * SQLLibraryBuild
 */
public class SQLLibraryBuild {
  private static final Logger LOG = LoggerFactory.getLogger(SQLLibraryBuild.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    if ((System.getProperty("SQLLib") == null) && (System.getProperty("SQLBuild") == null)) {
      LOG.info("Skipping SQL library build");
      return;
    }
    LOG.info("Starting SQL library build");
    ProcessBuilder makeCleanBuilder = new ProcessBuilder("make", "clean").redirectErrorStream(true);
    makeCleanBuilder.directory(new File(System.getProperty("user.dir") + "/tigon-sql/src/main/c"));
    LOG.info("Cleaning working directory");
    Process makeClean = makeCleanBuilder.start();
    printOutput(makeClean.getInputStream());
    ProcessBuilder makeInstallBuilder = new ProcessBuilder("make", "install").redirectErrorStream(true);
    makeInstallBuilder.directory(new File(System.getProperty("user.dir") + "/tigon-sql/src/main/c"));
    LOG.info("Compiling SQL binaries");
    Process makeInstall = makeInstallBuilder.start();
    printOutput(makeInstall.getInputStream());
    if (makeInstall.waitFor() != 0) {
      throw new RuntimeException("Failed to compile SQL Library. Make process exited with exit value " +
                                   makeInstall.exitValue());
    }
    Location target = new LocalLocationFactory().create(System.getProperty("user.dir") + "/tigon-sql/target");
    target.mkdirs();
    target.append("classes").mkdirs();
    ProcessBuilder createTarBuilder = new ProcessBuilder("tar",
                                                         "cvfz", "target/classes/" + Platform.libraryResource(),
                                                         "bin", "lib", "include", "cfg").redirectErrorStream(true);
    createTarBuilder.directory(new File(System.getProperty("user.dir") + "/tigon-sql/"));
    LOG.info("Creating tar ball");
    Process createTar = createTarBuilder.start();
    printOutput(createTar.getInputStream());
  }

  private static void printOutput(InputStream inputStream) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = br.readLine()) != null) {
      LOG.info(line);
    }
  }
}
