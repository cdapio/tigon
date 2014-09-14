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

import com.continuuity.tephra.TransactionManager;
import com.continuuity.tephra.TransactionSystemClient;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.app.guice.ProgramRunnerRuntimeModule;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.app.program.Programs;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricInMemoryModule;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.internal.app.runtime.BasicArguments;
import co.cask.tigon.internal.app.runtime.ProgramController;
import co.cask.tigon.internal.app.runtime.ProgramRunnerFactory;
import co.cask.tigon.internal.app.runtime.SimpleProgramOptions;
import co.cask.tigon.internal.flow.DefaultFlowSpecification;
import co.cask.tigon.logging.LogAppenderInitializer;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

/**
 *
 */
public class TestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static Injector injector;
  private static MetricsCollectionService metricsCollectionService;
  private static LogAppenderInitializer logAppenderInitializer;
  private static TransactionSystemClient txSystemClient;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static TransactionManager txService;
  private static DeployClient deployClient;
  private static ProgramRunnerFactory programRunnerFactory;

  protected FlowManager deployFlow(Class<? extends Flow> flowClz, Map<String, String> runtimeArgs,
                                   File...bundleEmbeddedJars) {
    Preconditions.checkNotNull(flowClz, "Flow class cannot be null");
    try {
      Object flowInstance = flowClz.newInstance();
      FlowSpecification flowSpec;

      if (flowInstance instanceof Flow) {
        Flow flow = (Flow) flowInstance;
        flowSpec = new DefaultFlowSpecification(flow.getClass().getName(), flow.configure());
      } else {
        throw new IllegalArgumentException("Flow class does not represent flow: " + flowClz.getName());
      }

      Location deployedJar = deployClient.deployFlow(flowSpec.getName(), flowClz, bundleEmbeddedJars);
      Program program = Programs.createWithUnpack(deployedJar, tmpFolder.newFolder());
      ProgramController controller = programRunnerFactory.create(ProgramRunnerFactory.Type.FLOW).run(
        program, new SimpleProgramOptions(program.getName(), new BasicArguments(), new BasicArguments(runtimeArgs)));
      return new DefaultFlowManager(controller);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    File localDataDir = tmpFolder.newFolder();
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
      createDataFabricModule(cConf),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new TestMetricsClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(TemporaryFolder.class).toInstance(tmpFolder);
        }
      }
    );

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    deployClient = new DeployClient(locationFactory);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    programRunnerFactory = injector.getInstance(ProgramRunnerFactory.class);
  }

  @AfterClass
  public static final void finish() {
    metricsCollectionService.stopAndWait();
    txService.stopAndWait();
  }

  private static Module createDataFabricModule(final CConfiguration cConf) {
    return new DataFabricInMemoryModule();
  }

  private static final class TestMetricsClientModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }

}
