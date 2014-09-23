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

package co.cask.tigon.cli;

import co.cask.tigon.app.program.Program;
import co.cask.tigon.app.program.Programs;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.queue.QueueName;
import co.cask.tigon.data.transaction.queue.QueueAdmin;
import co.cask.tigon.flow.DeployClient;
import co.cask.tigon.internal.app.runtime.ProgramOptionConstants;
import co.cask.tigon.internal.app.runtime.distributed.DistributedFlowletInstanceUpdater;
import co.cask.tigon.internal.app.runtime.distributed.FlowTwillProgramController;
import co.cask.tigon.internal.app.runtime.flow.FlowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Flow Operations Implementation for Distributed Mode.
 */
public class DistributedFlowOperations extends AbstractIdleService implements FlowOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowOperations.class);

  private final Location location;
  private final TwillRunnerService runnerService;
  private final DeployClient deployClient;
  private final File jarUnpackDir;
  private final QueueAdmin queueAdmin;

  @Inject
  public DistributedFlowOperations(LocationFactory locationFactory, TwillRunnerService runnerService,
                                   DeployClient deployClient, QueueAdmin queueAdmin) throws IOException {
    this.location = locationFactory.create(Constants.Location.FLOWJAR);
    location.mkdirs();
    this.runnerService = runnerService;
    this.deployClient = deployClient;
    this.jarUnpackDir = Files.createTempDir();
    this.queueAdmin = queueAdmin;
  }

  @Override
  protected void startUp() throws Exception {
    runnerService.startAndWait();
  }

  @Override
  public void startFlow(File jarPath, String className) {
//    try {
//      Location flowJar = deployClient.createFlowJar(jarPath, className, jarUnpackDir);
//      Program program = Programs.createWithUnpack(flowJar, jarUnpackDir);
//      String flowName = program.getSpecification().getName();
//      if (listAllFlows().contains(flowName)) {
//        throw new Exception("Flow with the same name is running! Stop or Delete the Flow before starting again");
//      }
//
//      Location jarInHDFS = location.append(flowName);
//      //Delete any existing JAR with the same flowName.
//      jarInHDFS.delete();
//      jarInHDFS.createNew();
//
//      //Copy the JAR to HDFS.
//      ByteStreams.copy(Locations.newInputSupplier(flowJar), Locations.newOutputSupplier(jarInHDFS));
//      //Start the Flow.
//      deployClient.startFlow(program, new HashMap<String, String>());
//    } catch (Exception e) {
//      LOG.error(e.getMessage(), e);
//    }
  }

  @Override
  public State getStatus(String flowName) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    sleepForZK(controllers);
    if (controllers.iterator().hasNext()) {
      State state = controllers.iterator().next().state();
      sleepForZK(state);
      return state;
    }
    return null;
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
      sleepForZK(iterable);
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
  public void deleteFlow(String flowName) {
    stopFlow(flowName);
    try {
      //Delete the Queues
      queueAdmin.clearAllForFlow(flowName, flowName);
      //Delete the JAR in HDFS
      Location jarinHDFS = location.append(flowName);
      jarinHDFS.delete();
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  @Override
  public List<String> getServices(String flowName) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    List<String> services = Lists.newArrayList();
    for (TwillController controller : controllers) {
      ResourceReport report = controller.getResourceReport();
      sleepForZK(report);
      services.addAll(report.getServices());
    }
    return services;
  }

  @Override
  public void setInstances(String flowName, String flowletName, int instanceCount) {
    Iterable<TwillController> controllers = lookupFlow(flowName);
    for (TwillController controller : controllers) {
      try {
        ResourceReport report = controller.getResourceReport();
        sleepForZK(report);
        int oldInstances = report.getResources().get(flowletName).size();
        Program program = Programs.create(location.append(flowName));
        Multimap<String, QueueName> consumerQueues = FlowUtils.configureQueue(program, program.getSpecification(),
                                                                           queueAdmin);
        DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(
          program, controller, queueAdmin, consumerQueues);
        FlowTwillProgramController flowController = new FlowTwillProgramController(program.getName(), controller,
                                                                                   instanceUpdater);
        Map<String, String> instanceOptions = Maps.newHashMap();
        instanceOptions.put("flowlet", flowletName);
        instanceOptions.put("newInstances", String.valueOf(instanceCount));
        instanceOptions.put("oldInstances", String.valueOf(oldInstances));
        flowController.command(ProgramOptionConstants.FLOWLET_INSTANCES, instanceOptions).get();
      } catch (Exception e) {
        LOG.warn(e.getMessage(), e);
      }
    }
  }

  @Override
  public Map<String, Integer> getFlowInfo(String flowName) {
    Map<String, Integer> flowletInfo = Maps.newHashMap();
    Iterable<TwillController> controllers = lookupFlow(flowName);
    for (TwillController controller : controllers) {
      ResourceReport report = controller.getResourceReport();
      sleepForZK(report);
      for (Map.Entry<String, Collection<TwillRunResources>> entry : report.getResources().entrySet()) {
        flowletInfo.put(entry.getKey(), entry.getValue().size());
      }
    }
    return flowletInfo;
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
    sleepForZK(iterable);
    return iterable;
  }

  private Iterable<TwillController> lookupFlow(String flowName) {
    Iterable<TwillController> iterable = runnerService.lookup(String.format("flow.%s", flowName));
    sleepForZK(iterable);
    return iterable;
  }

  private void sleepForZK(Object obj) {
    int count = 100;
    try {
      for (int i = 0; i < count; i++) {
        if (obj != null) {
          TimeUnit.MILLISECONDS.sleep(250);
          return;
        }
        TimeUnit.MILLISECONDS.sleep(250);
      }
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
