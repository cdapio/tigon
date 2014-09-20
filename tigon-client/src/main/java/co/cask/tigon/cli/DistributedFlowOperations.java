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

package co.cask.tigon.cli;

import co.cask.tigon.flow.DeployClient;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DistributedFlowOperations extends AbstractIdleService implements FlowOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowOperations.class);

  private final TwillRunnerService runnerService;
  private final DeployClient deployClient;
  private final File jarUnpackDir;

  @Inject
  public DistributedFlowOperations(TwillRunnerService runnerService, DeployClient deployClient) {
    this.runnerService = runnerService;
    this.deployClient = deployClient;
    this.jarUnpackDir = Files.createTempDir();
  }

  @Override
  protected void startUp() throws Exception {
    runnerService.startAndWait();
  }

  @Override
  public void deployFlow(File jarPath, String className) {
    try {
      deployClient.deployFlow(jarPath, className, jarUnpackDir);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public List<String> listAllFlows() {
    List<String> flowNames = Lists.newArrayList();
    for (TwillRunner.LiveInfo liveInfo : lookupService()) {
      String appName = liveInfo.getApplicationName();
      appName = appName.substring(appName.indexOf('.') + 1);
      flowNames.add(appName);
    }
    return flowNames;
  }

  @Override
  public List<InetSocketAddress> discover(String flowName, String service) {
    List<InetSocketAddress> endPoints = Lists.newArrayList();
    Iterable<TwillController> controllers = lookupFlow(flowName);
    for (TwillController controller : controllers) {
      Iterable<Discoverable> iterable = controller.discoverService(service);
      sleepForZK();
      for (Discoverable discoverable : iterable) {
        endPoints.add(discoverable.getSocketAddress());
      }
    }
    return endPoints;
  }

  @Override
  public void stopFlow(String flowName) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    for (TwillController controller : controllers) {
      controller.stopAndWait();
    }
  }

  @Override
  public void setInstances(String flowName, String flowInstance, int instanceCount) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    try {
      for (TwillController controller : controllers) {
        controller.changeInstances(flowInstance, instanceCount).get();
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  @Override
  public List<String> getFlowInfo(String flowName) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    for (TwillController controller : controllers) {
      ResourceReport report = controller.getResourceReport();
      sleepForZK();
      return new ArrayList<String>(report.getResources().keySet());
    }
    return null;
  }

  @Override
  public void addLogHandler(String flowName, PrintStream out) {
    Iterable<TwillController> iterable = lookupFlow(flowName);
    if (iterable.iterator().hasNext()) {
      TwillController controller = iterable.iterator().next();
      controller.addLogHandler(new PrinterLogHandler(new PrintWriter(out)));
    }
  }

  private Iterable<TwillRunner.LiveInfo> lookupService() {
    Iterable<TwillRunner.LiveInfo> iterable = runnerService.lookupLive();
    sleepForZK();
    return iterable;
  }

  private Iterable<TwillController> lookupFlow(String flowName) {
    Iterable<TwillController> iterable = runnerService.lookup(String.format("flow.%s", flowName));
    sleepForZK();
    return iterable;
  }

  private void sleepForZK() {
    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (InterruptedException e) {
      LOG.warn("Caught interrupted exception", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    runnerService.stopAndWait();
    FileUtils.deleteDirectory(jarUnpackDir);
  }
}
