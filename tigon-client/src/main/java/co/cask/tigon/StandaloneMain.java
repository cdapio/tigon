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

package co.cask.tigon;

import com.continuuity.tephra.TransactionManager;
import co.cask.tigon.app.guice.ProgramRunnerRuntimeModule;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricInMemoryModule;
import co.cask.tigon.flow.DeployClient;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.internal.app.runtime.ProgramController;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Tigon Standalone Main.
 */
public class StandaloneMain {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMain.class);

  private final CountDownLatch runLatch;
  private final File jarUnpackDir;
  private final File localDataDir;
  private final MetricsCollectionService metricsCollectionService;
  private final TransactionManager txService;
  private final DeployClient deployClient;
  private ProgramController controller;

  public StandaloneMain() {
    runLatch = new CountDownLatch(1);
    jarUnpackDir = Files.createTempDir();
    localDataDir = Files.createTempDir();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    Configuration hConf = new Configuration();

    Injector injector = Guice.createInjector(createModules(cConf, hConf));
    txService = injector.getInstance(TransactionManager.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    deployClient = injector.getInstance(DeployClient.class);
  }

  private static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("java -cp .:lib/* co.cask.tigon.StandaloneMain <path-to-JAR> <FlowClassName> [arguments]");
    out.println("Example: java -cp .:lib/* co.cask.tigon.StandaloneMain /home/user/tweetFlow-1.0.jar " +
                  "com.cname.main.TweetFlow --runtimeKey=value");
    out.println("");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  public static StandaloneMain createStandaloneMain() {
    return new StandaloneMain();
  }

  public static void main(String[] args) {
    System.out.println("Tigon Standalone Client");
    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      }

      if (args.length < 2) {
        usage(true);
      }

      File jarPath = new File(args[0]);
      String mainClassName = args[1];

      Map<String, String> runtimeArgs = null;
      try {
         runtimeArgs = fromPosixArray(Arrays.copyOfRange(args, 2, args.length));
      } catch (IllegalArgumentException e) {
        usage(true);
      }

      try {
        StandaloneMain main;
        main = createStandaloneMain();
        main.startUp(jarPath, mainClassName, runtimeArgs);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  public void startUp(File jarPath, String mainClassName, Map<String, String> runtimeArgs) throws Exception {
    txService.startAndWait();
    metricsCollectionService.startAndWait();
    addShutDownHook();
    Location deployJar = deployClient.createFlowJar(jarPath, mainClassName, jarUnpackDir);
    controller = deployClient.startFlow(deployJar, runtimeArgs);
    runLatch.await();
  }

  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
        }
      }
    });
  }

  public void shutDown() {
    try {
      if (controller != null) {
        controller.stop().get();
      }
      metricsCollectionService.stopAndWait();
      txService.stopAndWait();
      FileUtils.deleteDirectory(localDataDir);
      FileUtils.deleteDirectory(jarUnpackDir);
      runLatch.countDown();
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private static List<Module> createModules(CConfiguration cConf, Configuration hConf) {
    return ImmutableList.of(
      new DataFabricInMemoryModule(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new MetricsClientModule()
    );
  }

  private static final class MetricsClientModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }

  /**
   * Converts a POSIX compliant program argument array to a String-to-String Map.
   * @param args Array of Strings where each element is a POSIX compliant program argument (Ex: "--os=Linux" ).
   * @return Map of argument Keys and Values (Ex: Key = "os" and Value = "Linux").
   */
  private static Map<String, String> fromPosixArray(String[] args) {
    Map<String, String> kvMap = Maps.newHashMap();
    for (String arg : args) {
      kvMap.putAll(Splitter.on("--").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(arg));
    }
    return kvMap;
  }
}
