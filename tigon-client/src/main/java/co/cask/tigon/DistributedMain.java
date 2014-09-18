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

import co.cask.tigon.app.guice.ProgramRunnerRuntimeModule;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.app.program.Programs;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.data.runtime.DataFabricDistributedModule;
import co.cask.tigon.flow.DeployClient;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.guice.TwillModule;
import co.cask.tigon.guice.ZKClientModule;
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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;

/**
 * Tigon Distributed Main.
 */
public class DistributedMain {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedMain.class);
  private static final File jarUnpackDir = Files.createTempDir();

  private static Injector injector;
  private static MetricsCollectionService metricsCollectionService;
  private static DeployClient deployClient;
  private static ProgramRunnerFactory programRunnerFactory;
  private static ProgramController controller;
  private static TwillRunnerService twillRunnerService;
  private static CountDownLatch runLatch;

  static void usage(boolean error) {
    // Which output stream should we use?
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: ./run_standalone.sh <path-to-JAR> <FlowClassName>");
    out.println("Example: ./run_standalone.sh /home/user/tweetFlow-1.0.jar com.cname.main.TweetFlow");
    out.println("");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  private static void deploy(File jarPath, String classToLoad) throws Exception {
    Location deployedJar = deployClient.createJar(jarPath, classToLoad, jarUnpackDir);
    Program program = Programs.createWithUnpack(deployedJar, jarUnpackDir);
    controller = programRunnerFactory.create(ProgramRunnerFactory.Type.FLOW).run(
      program, new SimpleProgramOptions(program.getName(), new BasicArguments(), new BasicArguments()));
  }

  //Args expected : Path to the Main class;
  //TODO: Add Runtime args to the list
  public static void main(String[] args) {
    System.out.println("Tigon Distributed");
    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      }

      if (args.length != 2) {
        usage(true);
      }

      File jarPath = new File(args[0]);
      String mainClassName = args[1];
      try {
        init();
        deploy(jarPath, mainClassName);
        runLatch.await();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void init() throws Exception {
    runLatch = new CountDownLatch(1);
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    injector = Guice.createInjector(
      new DataFabricDistributedModule(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new TwillModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new TestMetricsClientModule()
    );

    Injector localInjector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getInMemoryModules()
    );

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    deployClient = localInjector.getInstance(DeployClient.class);
    programRunnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    twillRunnerService = injector.getInstance(TwillRunnerService.class);
    twillRunnerService.startAndWait();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          runLatch.countDown();
          finish();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
        }
      }
    });
  }

  public static void finish() throws Exception {
    if (controller != null) {
      controller.stop().get();
    }
    twillRunnerService.stopAndWait();
    metricsCollectionService.stopAndWait();
    FileUtils.deleteDirectory(jarUnpackDir);
  }

  private static final class TestMetricsClientModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }
}
