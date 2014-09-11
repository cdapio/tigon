/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.tigon.sql.flowlet;

import com.continuuity.api.metrics.Metrics;
import co.cask.tigon.sql.internal.DefaultInputFlowletConfigurer;
import co.cask.tigon.sql.internal.HealthInspector;
import co.cask.tigon.sql.internal.InputFlowletService;
import co.cask.tigon.sql.internal.LocalInputFlowletConfiguration;
import co.cask.tigon.sql.internal.MetricsRecorder;
import co.cask.tigon.sql.internal.ProcessMonitor;
import org.apache.twill.common.Services;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests InputFlowetConfiguration methods.
 */
public class InputFlowletConfigurationTest {
  private static final Logger LOG = LoggerFactory.getLogger(InputFlowletConfigurationTest.class);
  private static HealthInspector inspector;
  private static MetricsRecorder metricsRecorder;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static LocationFactory locationFactory;

  @BeforeClass
  public static void init() throws IOException {
    locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    inspector = new HealthInspector(new ProcessMonitor() {
      @Override
      public void notifyFailure(Set<String> errorProcessNames) {
        LOG.info("Heartbeat detection failure notified");
        for (String error : errorProcessNames) {
          LOG.error("No Heartbeat received from " + error);
        }
      }
    });
    metricsRecorder = new MetricsRecorder(new Metrics() {
      @Override
      public void count(String counterName, int delta) {
        LOG.info("[METRICS] CounterName : {}\tValue last second : {}", counterName, delta);
      }
    });
  }

  @Test
  public void testBasicFlowletConfig() throws Exception {
    testFlowletConfig(new InputFlowletBasic());
  }

  @Test
  public void testAllFieldTypeFlowletConfig() throws Exception {
    testFlowletConfig(new AllFieldTypeFlowlet());
  }

  @Test
  public void testTwoStreamFlowletConfig() throws Exception {
    testFlowletConfig(new InputFlowletWithTwoStreams());
  }

  //TODO : The tests don't fail if the processes fail to initiate. Refer HealthAndMetricsTest to add test failure
  //TODO (contd) condition after Mac shared memory fix for the SQL Compiler
  private void testFlowletConfig(AbstractInputFlowlet flowlet) throws IOException, InterruptedException {
    // Create base dir
    Location baseDir = locationFactory.create(TEMP_FOLDER.newFolder().toURI());
    baseDir.mkdirs();

    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    InputFlowletSpecification spec = configurer.createInputFlowletSpec();

    InputFlowletConfiguration inputFlowletConfiguration = new LocalInputFlowletConfiguration(baseDir, spec);
    Location binDir = inputFlowletConfiguration.createStreamEngineProcesses();

    GDATRecordQueue recordQueue = new GDATRecordQueue();

    InputFlowletService driver = new InputFlowletService(binDir, spec, inspector, metricsRecorder, recordQueue);

    try {
      driver.startAndWait();
      inspector.startAndWait();
      //TODO: Test needs to be enhanced to have end-to-end data ingestion and output check.
      TimeUnit.SECONDS.sleep(10);
    } finally {
      Services.chainStop(inspector, driver);
    }
  }
}
