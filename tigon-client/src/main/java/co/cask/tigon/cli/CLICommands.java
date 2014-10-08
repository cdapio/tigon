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

package co.cask.tigon.cli;

/**
 * Commands used in Command Line Interface.
 */
public enum CLICommands {

  START (2, "Starts a Flow with Runtime Args", "<path-to-jar> <flow-classname> [ --key1=v1 --key2=v2 ]"),
  LIST (0, "Lists all the Flows which are currently running", ""),
  STOP (1, "Stops a Flow", "<flowname>"),
  DELETE (1, "Stops and Deletes the Queues for a Flow", "<flowname>"),
  SERVICEINFO (1, "Prints all Services announced in a Flow", "<flowname>"),
  DISCOVER (2, "Discovers a Service endpoint for a Flow", "<flowname> <servicename>"),
  SET (3, "Set the number of Flowlet Instances for a Flow", "<flowname> <flowletname> <instance-count>"),
  FLOWLETINFO (1, "Prints the Flowlet Names and the corresponding Instance Count", "<flowname>"),
  STATUS (1, "Shows the Status of the Flow", "<flowname>"),
  VERSION (0, "Shows the Version of Tigon", ""),
  SHOWLOGS (1, "Shows the live logs of the Flow", "<flowname>"),
  HELP (1, "Shows the usage for that given command", "<command>"),
  QUIT(0, "Quit the Tigon Client", "");

  private int argCount;
  private String description;
  private String args;

  CLICommands(int argCount, String description, String args) {
    this.argCount = argCount;
    this.description = description;
    this.args = args;
  }

  public int getArgCount() {
    //Including the command.
    return argCount + 1;
  }

  public String printHelp() {
    return String.format("Command Description : %s\nUsage : %s %s\n", description, this.toString().toLowerCase(), args);
  }
}
