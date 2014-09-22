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

package co.cask.tigon.internal.app.runtime.distributed;

import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.FlowletDefinition;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.app.program.ProgramType;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.queue.QueueName;
import co.cask.tigon.data.transaction.queue.QueueAdmin;
import co.cask.tigon.internal.app.runtime.ProgramController;
import co.cask.tigon.internal.app.runtime.ProgramOptions;
import co.cask.tigon.internal.app.runtime.flow.FlowUtils;
import co.cask.tigon.twill.AbortOnTimeoutEventHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public final class DistributedFlowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowProgramRunner.class);
  private final QueueAdmin queueAdmin;

  @Inject
  DistributedFlowProgramRunner(TwillRunner twillRunner, Configuration hConfig, CConfiguration cConfig,
                               QueueAdmin queueAdmin) {
    super(twillRunner, hConfig, cConfig);
    this.queueAdmin = queueAdmin;
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     File hConfFile, File cConfFile, ApplicationLauncher launcher) {
    // Extract and verify parameters
    FlowSpecification flowSpec = program.getSpecification();
    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.FLOW, "Only FLOW process type is supported.");

    try {
      Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getName());

      for (FlowletDefinition flowletDefinition : flowSpec.getFlowlets().values()) {
        int maxInstances = flowletDefinition.getFlowletSpec().getMaxInstances();
        Preconditions.checkArgument(flowletDefinition.getInstances() <= maxInstances,
                                    "Flowlet %s can have a maximum of %s instances",
                                    flowletDefinition.getFlowletSpec().getName(), maxInstances);
      }

      LOG.info("Configuring flowlets queues");
      Multimap<String, QueueName> flowletQueues = FlowUtils.configureQueue(program, flowSpec, queueAdmin);

      // Launch flowlet program runners
      LOG.info("Launching distributed flow: " + program.getName() + ":" + flowSpec.getName());

      TwillController controller = launcher.launch(new FlowTwillApplication(program, flowSpec,
                                                                            hConfFile, cConfFile, eventHandler));
      DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(program, controller,
                                                                                                queueAdmin,
                                                                                                flowletQueues);
      return new FlowTwillProgramController(program.getName(), controller, instanceUpdater).startListen();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }
}
