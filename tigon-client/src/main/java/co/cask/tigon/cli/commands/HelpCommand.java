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
import com.google.common.base.Supplier;

import java.io.PrintStream;

/**
 * Command to get the list of available commands and their descriptions.
 */
public class HelpCommand implements Command {
  private final Supplier<Iterable<Command>> commands;

  public HelpCommand(Supplier<Iterable<Command>> commands) {
    this.commands = commands;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    for (Command command : commands.get()) {
      output.printf("%s: %s\n", command.getPattern(), command.getDescription());
    }
  }

  @Override
  public String getPattern() {
    return "help";
  }

  @Override
  public String getDescription() {
    return "Prints the available CLI commands and their descriptions";
  }
}
