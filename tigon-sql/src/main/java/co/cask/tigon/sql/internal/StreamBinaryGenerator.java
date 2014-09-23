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
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import co.cask.tigon.sql.util.Platform;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Generate Stream Engine Binaries.
 */
public class StreamBinaryGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBinaryGenerator.class);
  private final Location dir;
  private final InputFlowletSpecification spec;

  public StreamBinaryGenerator(Location dir, InputFlowletSpecification spec) {
    this.dir = dir;
    this.spec = spec;
  }

  public Location createStreamProcesses() {
    Location configDir = null;
    try {
      configDir = createStreamLibrary(dir);
      CompileStreamBinaries compileBinaries = new CompileStreamBinaries(configDir);
      StreamConfigGenerator generator = new StreamConfigGenerator(spec);
      Location outputSpec = createFile(configDir, "output_spec.cfg");
      Location packetSchema = createFile(configDir, "packet_schema.txt");
      Location ifresXml = createFile(configDir, "ifres.xml");
      Map.Entry<String, String> ifqContent = generator.generateHostIfq();
      String ifqFile = String.format("%s.ifq", ifqContent.getKey());
      Location hostIfq = createFile(configDir, ifqFile);

      writeToLocation(outputSpec, generator.generateOutputSpec());
      writeToLocation(packetSchema, generator.generatePacketSchema());
      writeToLocation(ifresXml, generator.generateIfresXML());
      writeToLocation(hostIfq, generator.generateHostIfq().getValue());

      Map<String, String> gsqlFiles = generator.generateQueryFiles();
      for (Map.Entry<String, String> gsqlFile : gsqlFiles.entrySet()) {
        String fileName = String.format("%s.gsql", gsqlFile.getKey());
        Location file = createFile(configDir, fileName);
        writeToLocation(file, gsqlFile.getValue());
      }

      compileBinaries.generateBinaries();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      Throwables.propagate(t);
    }
    return configDir;
  }

  private Location createStreamLibrary(Location dir) throws IOException, ArchiveException {
    if (!dir.exists()) {
      dir.mkdirs();
    }

    //Get the library zip, copy it to temp dir, unzip it
    String libFile = Platform.libraryResource();
    Location libZip = dir.append(libFile);
    libZip.createNew();
    copyResourceFileToDir(libFile, libZip);
    unzipFile(libZip);

    //Create directory structure to place the Stream Engine Config Files
    Location workDir = dir.append("work");
    workDir.mkdirs();
    Location queryDir = workDir.append("query");
    queryDir.mkdirs();
    File qDir = new File(queryDir.toURI().getPath());
    Files.copy(new File(dir.append("cfg").append("external_fcns.def").toURI().getPath()), qDir);
    Files.copy(new File(dir.append("cfg").append("internal_fcn.def").toURI().getPath()), qDir);
    return queryDir;
  }

  private Location createFile(Location dir, String name) throws IOException {
    Location file = dir.append(name);
    file.createNew();
    return file;
  }

  private void writeToLocation(Location loc, String content) throws IOException {
    CharStreams.write(content, CharStreams.newWriterSupplier(Locations.newOutputSupplier(loc), Charsets.UTF_8));
  }

  private void copyResourceFileToDir(String fileName, Location libZip) throws IOException {
    InputStream ifres = getClass().getResourceAsStream("/" + fileName);
    ByteStreams.copy(ifres, Locations.newOutputSupplier(libZip));
    ifres.close();
  }

  private void unzipFile(Location libZip) throws IOException, ArchiveException {
    String path = libZip.toURI().getPath();
    String outDir = Locations.getParent(libZip).toURI().getPath();
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
