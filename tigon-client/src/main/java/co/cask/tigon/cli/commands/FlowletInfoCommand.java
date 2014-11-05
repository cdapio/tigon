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
import org.apache.twill.api.TwillRunResources;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;

/**
 * Command to get the Flowlet Names (and their number of instances) of a Flow.
 */
public class FlowletInfoCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public FlowletInfoCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    String flowName = arguments.get("flow-name");
    printStream.println(String.format("%-20s %s", "Flowlet Name", "Instance Count"));
    Map<String, Collection<TwillRunResources>> flowletInfoMap = operations.getFlowInfo(flowName);
    for (String flowletName : flowletInfoMap.keySet()) {
      printStream.println(String.format("%-20s %s", flowletName, flowletInfoMap.get(flowletName).size()));
    }
  }

  @Override
  public String getPattern() {
    return "flowletinfo <flow-name>";
  }

  @Override
  public String getDescription() {
    return "Prints the Flowlet Names and the corresponding Instance Count";
  }
}
