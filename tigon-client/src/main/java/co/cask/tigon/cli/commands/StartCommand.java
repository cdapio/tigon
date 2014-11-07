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
import co.cask.tigon.flow.DeployClient;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Command to start a Flow.
 */
public class StartCommand implements Command {
  private final FlowOperations operations;

  @Inject
  public StartCommand(FlowOperations operations) {
    this.operations = operations;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    startFlow(arguments, output, false);
  }

  protected void startFlow(Arguments arguments, PrintStream output, boolean debug) throws Exception {
    String jarPath = arguments.get("path-to-jar");
    String className = arguments.get("flow-classname");
    Map<String, String> runtimeArgs = Maps.newHashMap();
    if (arguments.hasArgument("runtime-args")) {
      String args = arguments.get("runtime-args");
      List<String> argList = Lists.newArrayList(Splitter.on(',').omitEmptyStrings().split(args));
      runtimeArgs = DeployClient.fromPosixArray(argList);
    }
    operations.startFlow(new File(jarPath), className, runtimeArgs, debug);
  }

  protected String basePattern() {
    return "<path-to-jar> <flow-classname> [<runtime-args>]";
  }

  @Override
  public String getPattern() {
    return "start " + basePattern();
  }

  @Override
  public String getDescription() {
    return "Starts a Flow with optional Runtime Args. Runtime Args are specified as : '--key1=v1, --key2=v2' ";
  }
}
