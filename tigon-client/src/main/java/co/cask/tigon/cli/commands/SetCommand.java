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

package co.cask.tigon.cli.commands;

import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.tigon.cli.FlowOperations;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Command to set the instance count of a Flowlet in a Flow.
 */
public class SetCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public SetCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    String flowName = arguments.get("flow-name");
    String flowletName = arguments.get("flowlet-name");
    String instanceCount = arguments.get("instance-count");
    operations.setInstances(flowName, flowletName, Integer.valueOf(instanceCount));
  }

  @Override
  public String getPattern() {
    return "set <flow-name> <flowlet-name> <instance-count>";
  }

  @Override
  public String getDescription() {
    return "Set the number of Flowlet Instances for a Flow";
  }
}
