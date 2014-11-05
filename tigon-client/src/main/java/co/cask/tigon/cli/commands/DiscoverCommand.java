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
import java.net.InetSocketAddress;

/**
 * Command to discover the endpoint of a Service announced in a Flow.
 */
public class DiscoverCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public DiscoverCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    String flowName = arguments.get("flow-name");
    String serviceName = arguments.get("service-name");
    for (InetSocketAddress socketAddress : operations.discover(flowName, serviceName)) {
      printStream.println(String.format("%s:%s", socketAddress.getHostName(), socketAddress.getPort()));
    }
  }

  @Override
  public String getPattern() {
    return "discover <flow-name> <service-name>";
  }

  @Override
  public String getDescription() {
    return "Discovers a Service endpoint for a Flow";
  }
}
