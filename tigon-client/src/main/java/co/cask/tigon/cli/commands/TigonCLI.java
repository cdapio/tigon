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

import co.cask.common.cli.CLI;
import co.cask.common.cli.Command;
import co.cask.common.cli.completers.StringsCompleter;
import co.cask.tigon.cli.FlowOperations;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Tigon CLI Class.
 */
public class TigonCLI {
  private final List<Command> commands;
  private final Map<String, Completer> completerMap;
  private final FlowOperations operations;
  private final HelpCommand helpCommand;

  @Inject
  public TigonCLI(Injector injector) {
    operations = injector.getInstance(FlowOperations.class);
    commands = ImmutableList.of(
      injector.getInstance(DebugInfoCommand.class),
      injector.getInstance(DebugStartCommand.class),
      injector.getInstance(DeleteCommand.class),
      injector.getInstance(DiscoverCommand.class),
      injector.getInstance(FlowletInfoCommand.class),
      injector.getInstance(ListCommand.class),
      injector.getInstance(QuitCommand.class),
      injector.getInstance(ServiceInfoCommand.class),
      injector.getInstance(SetCommand.class),
      injector.getInstance(ShowLogsCommand.class),
      injector.getInstance(StartCommand.class),
      injector.getInstance(StatusCommand.class),
      injector.getInstance(StopCommand.class),
      injector.getInstance(VersionCommand.class));

    helpCommand = new HelpCommand(new Supplier<Iterable<Command>>() {
      @Override
      public Iterable<Command> get() {
        return commands;
      }
    });

    completerMap = ImmutableMap.of(
      "flow-name", new StringsCompleter() {
        @Override
        protected Supplier<Collection<String>> getStringsSupplier() {
          return Suppliers.<Collection<String>>ofInstance(operations.listAllFlows());
        }
      },
      "path-to-jar", new FileNameCompleter(),
      "flowlet-name", new StringsCompleter() {
        @Override
        protected Supplier<Collection<String>> getStringsSupplier() {
          List<String> flowletNames = Lists.newArrayList();
          for (String flowName : operations.listAllFlows()) {
            for (String flowletName : operations.getFlowInfo(flowName).keySet()) {
              flowletNames.add(String.format("%s.%s", flowName, flowletName));
            }
          }
          return Suppliers.<Collection<String>>ofInstance(flowletNames);
        }
      },
      "service-name", new StringsCompleter() {
        @Override
        protected Supplier<Collection<String>> getStringsSupplier() {
          List<String> serviceNames = Lists.newArrayList();
          for (String flowName : operations.listAllFlows()) {
            for (String serviceName : operations.getServices(flowName)) {
              serviceNames.add(String.format("%s.%s", flowName, serviceName));
            }
          }
          return Suppliers.<Collection<String>>ofInstance(serviceNames);
        }
      }
    );
  }

  public void start(PrintStream out) throws IOException {
    CLI cli = new CLI<Command>(Iterables.concat(commands, ImmutableList.<Command>of(helpCommand)), completerMap);
    cli.getReader().setPrompt("tigon> ");
    cli.startInteractiveMode(out);
  }
}
