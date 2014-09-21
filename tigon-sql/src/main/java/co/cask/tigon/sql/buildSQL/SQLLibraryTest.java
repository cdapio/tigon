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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * SQLLibraryTest
 */
public class SQLLibraryTest {
  private static final Logger LOG = LoggerFactory.getLogger(SQLLibraryTest.class);

  public static void main(String[] args) throws IOException {
    if (System.getProperty("maven.test.skip") != null || (System.getProperty("skipTests") != null) ||
      (System.getProperty("skipSQLLib") != null) || (System.getProperty("skipSQLTests") != null)) {
      return;
    }
    ProcessBuilder testSQLLibraryBuilder = new ProcessBuilder("perl", "run_test.pl").redirectErrorStream(true);
    testSQLLibraryBuilder.directory(new File(System.getProperty("user.dir") + "/tigon-sql/src/test/scripts"));
    LOG.info("Testing SQL library");
    Process testSQLLibrary = testSQLLibraryBuilder.start();
    printOutput(testSQLLibrary.getInputStream());
    if (testSQLLibrary.exitValue() != 0) {
      throw new RuntimeException("SQL Library tests FAILED! Test exited with exit value " + testSQLLibrary.exitValue() +
                                   ". Logs available at ./tigon-sql/src/test/scripts");
    }
    LOG.info("Finished testing SQL library");
  }

  private static void printOutput(InputStream inputStream) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = br.readLine()) != null) {
      LOG.info(line);
    }
  }
}
