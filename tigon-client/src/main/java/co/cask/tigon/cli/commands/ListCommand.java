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
import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;

/**
 * Command that lists all the running Flows.
 */
public class ListCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public ListCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    output.println(StringUtils.join(operations.listAllFlows(), ", "));
  }

  @Override
  public String getPattern() {
    return "list";
  }

  @Override
  public String getDescription() {
    return "Lists all the Flows which are currently running";
  }
}
