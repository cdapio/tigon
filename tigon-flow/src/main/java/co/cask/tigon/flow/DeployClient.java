/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.tigon.flow;

import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.app.program.ManifestFields;
import co.cask.tigon.internal.app.FlowSpecificationAdapter;
import co.cask.tigon.internal.flow.DefaultFlowSpecification;
import co.cask.tigon.internal.io.ReflectionSchemaGenerator;
import co.cask.tigon.lang.ApiResourceListHolder;
import co.cask.tigon.lang.ClassLoaders;
import co.cask.tigon.lang.jar.ProgramClassLoader;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Client tool for AppFabricHttpHandler.
 */
public class DeployClient {
  private static final Logger LOG = LoggerFactory.getLogger(DeployClient.class);
  private static final Gson GSON = new Gson();

  private final LocationFactory locationFactory;

  @Inject
  public DeployClient(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link java.util.jar.Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getName());
    return manifest;
  }

  private static void expandJar(File jarPath, File unpackDir) throws Exception {
    JarFile jar = new JarFile(jarPath);
    Enumeration enumEntries = jar.entries();
    while (enumEntries.hasMoreElements()) {
      JarEntry file = (JarEntry) enumEntries.nextElement();
      File f = new File(unpackDir + File.separator + file.getName());
      if (file.isDirectory()) {
        f.mkdirs();
        continue;
      } else {
        f.getParentFile().mkdirs();
      }
      InputStream is = jar.getInputStream(file);
      FileOutputStream fos = new FileOutputStream(f);
      while (is.available() > 0) {
        fos.write(is.read());
      }
      fos.close();
      is.close();
    }
  }

  public Location createJar(File jarPath, String classToLoad, File jarUnpackDir) throws Exception {
    expandJar(jarPath, jarUnpackDir);
    URL jarURL = jarUnpackDir.toURI().toURL();
    ProgramClassLoader filterClassLoader = ClassLoaders.newProgramClassLoader(jarUnpackDir,
                                                                              ApiResourceListHolder.getResourceList(),
                                                                              DeployClient.class.getClassLoader());
    URLClassLoader classLoader = new URLClassLoader(new URL[]{jarURL}, filterClassLoader);
    Class<?> clz = classLoader.loadClass(classToLoad);
    if (!(clz.newInstance() instanceof Flow)) {
      throw new Exception("Expected Flow class");
    }
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    return deployFlow(clz);
  }

  Location deployFlow(Class<?> flowClz, File... bundleEmbeddedJars)
    throws Exception {
    Preconditions.checkNotNull(flowClz, "Flow cannot be null.");
    Location deployedJar = locationFactory.create(createDeploymentJar(
      locationFactory, flowClz, bundleEmbeddedJars).toURI());
    LOG.info("Created deployedJar at {}", deployedJar.toURI().toASCIIString());
    return deployedJar;
  }

  private static InputSupplier<InputStream> getInputSupplier(final FlowSpecification flowSpec) {
    return new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        String json = FlowSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(flowSpec);
        return new ByteArrayInputStream(json.getBytes(Charsets.UTF_8));
      }
    };
  }

  private static File createDeploymentJar(LocationFactory locationFactory, Class<?> clz, File...bundleEmbeddedJars)
    throws IOException, InstantiationException, IllegalAccessException {

    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.tigon.api",
                                                                         "org.apache.hadoop",
                                                                         "org.apache.hbase"));
    Location jarLocation = locationFactory.create(clz.getName()).getTempFile(".jar");
    bundler.createBundle(jarLocation, clz);

    Location deployJar = locationFactory.create(clz.getName()).getTempFile(".jar");

    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, clz.getName());
    manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, ManifestFields.MANIFEST_SPEC_FILE);

    // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
    // by the ApplicationBundler inside Twill.
    JarOutputStream jarOutput = new JarOutputStream(deployJar.getOutputStream(), manifest);
    try {
      JarInputStream jarInput = new JarInputStream(jarLocation.getInputStream());
      try {
        JarEntry jarEntry = jarInput.getNextJarEntry();
        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          if (!entryName.equals("classes/") && !entryName.endsWith("META-INF/MANIFEST.MF")) {
            if (entryName.startsWith("classes/")) {
              jarEntry = new JarEntry(entryName.substring("classes/".length()));
            } else {
              jarEntry = new JarEntry(entryName);
            }
            jarOutput.putNextEntry(jarEntry);

            if (!isDir) {
              ByteStreams.copy(jarInput, jarOutput);
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }
      } finally {
        jarInput.close();
      }

      for (File embeddedJar : bundleEmbeddedJars) {
        JarEntry jarEntry = new JarEntry("lib/" + embeddedJar.getName());
        jarOutput.putNextEntry(jarEntry);
        Files.copy(embeddedJar, jarOutput);
      }

      JarEntry jarEntry = new JarEntry(ManifestFields.MANIFEST_SPEC_FILE);
      jarOutput.putNextEntry(jarEntry);
      Flow flow = (Flow) clz.newInstance();
      FlowSpecification flowSpec = new DefaultFlowSpecification(clz.getClass().getName(), flow.configure());
      ByteStreams.copy(getInputSupplier(flowSpec), jarOutput);
    } finally {
      jarOutput.close();
    }

    return new File(deployJar.toURI());
  }

}
