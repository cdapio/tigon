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
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunResources;

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Command to retrieve remote debug hostname and port.
 */
public class DebugInfoCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public DebugInfoCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String flowletName = arguments.get("flowlet-name");
    List<String> nameParts = Lists.newArrayList(Splitter.on(".").trimResults().split(flowletName));
    Map<String, Collection<TwillRunResources>> flowletInfoMap = operations.getFlowInfo(nameParts.get(0));
    for (TwillRunResources resources : flowletInfoMap.get(nameParts.get(1))) {
      output.println(String.format("%s:%s", resources.getHost(), resources.getDebugPort()));
    }
  }

  @Override
  public String getPattern() {
    return "debuginfo <flowlet-name>";
  }

  @Override
  public String getDescription() {
    return "Prints the debug hostname and port for the Flowlet. Flow should have been started in debug mode.";
  }
}
