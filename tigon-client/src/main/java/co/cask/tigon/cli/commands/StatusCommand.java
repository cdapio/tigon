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
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Command to retrieve the status of a Flow.
 */
public class StatusCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public StatusCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    String flowName = arguments.get("flow-name");
    Service.State state = operations.getStatus(flowName);
    String status = (state != null) ? state.toString() : "NOT FOUND";
    printStream.println(status);
  }

  @Override
  public String getPattern() {
    return "status <flow-name>";
  }

  @Override
  public String getDescription() {
    return "Shows the Status of the Flow";
  }
}
