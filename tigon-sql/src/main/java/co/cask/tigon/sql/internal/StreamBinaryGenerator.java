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

import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import co.cask.tigon.sql.util.Platform;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Generate Stream Engine Binaries.
 */
public class StreamBinaryGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBinaryGenerator.class);
  private final File dir;
  private final InputFlowletSpecification spec;

  public StreamBinaryGenerator(File dir, InputFlowletSpecification spec) {
    this.dir = dir;
    this.spec = spec;
  }

  public File createStreamProcesses() {
    try {
      File configDir = createStreamLibrary(dir);
      CompileStreamBinaries compileBinaries = new CompileStreamBinaries(configDir);
      StreamConfigGenerator generator = new StreamConfigGenerator(spec);
      File outputSpec = createFile(configDir, "output_spec.cfg");
      File packetSchema = createFile(configDir, "packet_schema.txt");
      File ifresXml = createFile(configDir, "ifres.xml");
      Map.Entry<String, String> ifqContent = generator.generateHostIfq();
      String ifqFile = String.format("%s.ifq", ifqContent.getKey());
      File hostIfq = createFile(configDir, ifqFile);

      writeToLocation(outputSpec, generator.generateOutputSpec());
      writeToLocation(packetSchema, generator.generatePacketSchema());
      writeToLocation(ifresXml, generator.generateIfresXML());
      writeToLocation(hostIfq, generator.generateHostIfq().getValue());

      Map<String, String> gsqlFiles = generator.generateQueryFiles();
      Collection<String> fileContent = gsqlFiles.values();
      File file = createFile(configDir, Constants.GSQL_FILE);
      writeToLocation(file, Joiner.on(";\n").join(fileContent));

      compileBinaries.generateBinaries();
      return configDir;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      Throwables.propagate(t);
      return null;
    }
  }

  private File createStreamLibrary(File dir) throws IOException, ArchiveException {
    if (!dir.exists()) {
      dir.mkdirs();
    }

    //Get the library zip, copy it to temp dir, unzip it
    String libFile = Platform.libraryResource();
    File libZip = new File(dir, libFile);
    libZip.createNewFile();
    copyResourceFileToDir(libFile, libZip);
    unzipFile(libZip);

    //Create directory structure to place the Stream Engine Config Files
    File workDir = new File(dir, "work");
    workDir.mkdirs();
    File queryDir = new File(workDir, "query");
    queryDir.mkdirs();
    FileUtils.copyFileToDirectory(new File(dir, "cfg/external_fcns.def"), queryDir);
    FileUtils.copyFileToDirectory(new File(dir, "cfg/internal_fcn.def"), queryDir);
    return queryDir;
  }

  private File createFile(File dir, String name) throws IOException {
    File file = new File(dir, name);
    file.createNewFile();
    return file;
  }

  private void writeToLocation(File loc, String content) throws IOException {
    CharStreams.write(content, CharStreams.newWriterSupplier(Files.newOutputStreamSupplier(loc), Charsets.UTF_8));
  }

  private void copyResourceFileToDir(String fileName, File libZip) throws IOException {
    InputStream ifres = getClass().getResourceAsStream("/" + fileName);
    ByteStreams.copy(ifres, Files.newOutputStreamSupplier(libZip));
    ifres.close();
  }

  private void unzipFile(File libZip) throws IOException, ArchiveException {
    String path = libZip.toURI().getPath();
    String outDir = libZip.getParentFile().getPath();
    TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(
      new GZIPInputStream(new FileInputStream(path)));
    try {
      TarArchiveEntry entry = archiveInputStream.getNextTarEntry();
      while (entry != null) {
        File destFile = new File(outDir, entry.getName());
        destFile.getParentFile().mkdirs();
        if (!entry.isDirectory()) {
          ByteStreams.copy(archiveInputStream, Files.newOutputStreamSupplier(destFile));
          //TODO: Set executable permission based on entry.getMode()
          destFile.setExecutable(true, false);
        }
        entry = archiveInputStream.getNextTarEntry();
      }
    } finally {
      archiveInputStream.close();
    }
  }
}
