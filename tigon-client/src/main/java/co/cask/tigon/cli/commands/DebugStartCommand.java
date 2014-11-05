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
import co.cask.tigon.cli.FlowOperations;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Command to start a Flow in debug mode.
 */
public class DebugStartCommand extends StartCommand {

  @Inject
  public DebugStartCommand(FlowOperations operations) {
    super(operations);
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    startFlow(arguments, output, true);
  }

  @Override
  public String getPattern() {
    return "debug " + basePattern();
  }

  @Override
  public String getDescription() {
    return "Starts a Flow with optional Runtime Args in debug mode." +
      " Runtime Args are specified as : '--key1=v1, --key2=v2'";
  }
}
