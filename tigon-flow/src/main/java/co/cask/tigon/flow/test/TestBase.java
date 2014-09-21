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
package co.cask.tigon.flow.test;

import com.continuuity.tephra.TransactionManager;
import co.cask.tigon.api.flow.Flow;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

/**
 * Tigon Test Base to Test Flows.
 */
public class TestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static MetricsCollectionService metricsCollectionService;
  private static TransactionManager txService;
  private static DeployClient deployClient;

  protected FlowManager deployFlow(Class<? extends Flow> flowClz, Map<String, String> runtimeArgs,
                                   File...bundleEmbeddedJars) {
    Preconditions.checkNotNull(flowClz, "Flow class cannot be null");
    try {
      Location deployJar = deployClient.jarForTestBase(flowClz, bundleEmbeddedJars);
      ProgramController controller = deployClient.startFlow(deployJar, runtimeArgs);
      return new DefaultFlowManager(controller);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    File localDataDir = tmpFolder.newFolder();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());

    Configuration hConf = new Configuration();

    Injector injector = Guice.createInjector(
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
    deployClient = injector.getInstance(DeployClient.class);
  }

  @AfterClass
  public static void finish() {
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
