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
package co.cask.tigon;

import com.continuuity.tephra.TransactionManager;
import com.continuuity.tephra.TransactionSystemClient;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.app.guice.ProgramRunnerRuntimeModule;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.app.program.Programs;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricInMemoryModule;
import co.cask.tigon.flow.DeployClient;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.internal.app.runtime.BasicArguments;
import co.cask.tigon.internal.app.runtime.ProgramController;
import co.cask.tigon.internal.app.runtime.ProgramRunnerFactory;
import co.cask.tigon.internal.app.runtime.SimpleProgramOptions;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Tigon Standalone Main.
 */
public class StandaloneMain {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMain.class);
  private static final File localDataDir = Files.createTempDir();
  private static final File jarUnpackDir = Files.createTempDir();
  private static Injector injector;
  private static MetricsCollectionService metricsCollectionService;
  private static TransactionSystemClient txSystemClient;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static TransactionManager txService;
  private static LocationFactory locationFactory;
  private static DeployClient deployClient;
  private static ProgramRunnerFactory programRunnerFactory;
  private static ProgramController controller;

  static void usage(boolean error) {
    // Which output stream should we use?
    PrintStream out = (error ? System.err : System.out);
    out.println("Print help message here");
    out.println("");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  private static void expandJar(String jarPath, File unpackDir) throws Exception {
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

  private static void deploy(String jarPath, String classToLoad) throws Exception {
    expandJar(jarPath, jarUnpackDir);
    URL jarURL = new URL("file://" + jarUnpackDir.getAbsolutePath() + "/");
    URLClassLoader classLoader = new URLClassLoader(new URL[] { jarURL }, StandaloneMain.class.getClassLoader());
    Class<?> clz = classLoader.loadClass(classToLoad);
    if (!(clz.newInstance() instanceof Flow)) {
      throw new Exception("Expected Flow class");
    }
    Location deployedJar = deployClient.deployFlow(clz);
    Program program = Programs.createWithUnpack(deployedJar, jarUnpackDir);
    controller = programRunnerFactory.create(ProgramRunnerFactory.Type.FLOW).run(
      program, new SimpleProgramOptions(program.getName(), new BasicArguments(), new BasicArguments()));
  }

  //Args expected : Path to the Main class;
  //TODO: Add Runtime args to the list
  public static void main(String[] args) {
    String jarPath;
    String mainClassName;
    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      }

      if (args.length != 2) {
        usage(true);
      }

      jarPath = args[0];
      mainClassName = args[1];
      try {
        init();
        deploy(jarPath, mainClassName);
      } catch (Exception e) {
        System.err.println(e);
      }
    }
  }

  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    Configuration hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    injector = Guice.createInjector(
      new DataFabricInMemoryModule(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new TestMetricsClientModule()
    );

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    locationFactory = injector.getInstance(LocationFactory.class);
    deployClient = new DeployClient(locationFactory);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    programRunnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          finish();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
        }
      }
    });
  }

  public static void finish() {
    if (controller != null) {
      controller.stop();
    }
    metricsCollectionService.stopAndWait();
    txService.stopAndWait();
  }

  private static final class TestMetricsClientModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }

}
