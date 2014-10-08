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

package co.cask.tigon;

import co.cask.tigon.app.guice.ProgramRunnerRuntimeModule;
import co.cask.tigon.cli.CLICommands;
import co.cask.tigon.cli.DistributedFlowOperations;
import co.cask.tigon.cli.FlowOperations;
import co.cask.tigon.cli.InvalidCLIArgumentException;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricDistributedModule;
import co.cask.tigon.flow.DeployClient;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.guice.TwillModule;
import co.cask.tigon.guice.ZKClientModule;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tigon Distributed Main.
 */
public class DistributedMain {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedMain.class);

  private final File localDataDir;
  private final FlowOperations flowOperations;
  private final ConsoleReader consoleReader;

  public DistributedMain(String zkQuorumString, String rootNamespace) throws IOException {
    localDataDir = Files.createTempDir();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkQuorumString);
    cConf.set(Constants.Location.ROOT_NAMESPACE, rootNamespace);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.reloadConfiguration();
    Configuration hConf = HBaseConfiguration.create();

    Injector injector = Guice.createInjector(createModules(cConf, hConf));
    flowOperations = injector.getInstance(FlowOperations.class);
    consoleReader = new ConsoleReader();
  }

  private static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println(
      "Usage:   java -cp lib/*:<hadoop/hbase classpath> co.cask.tigon.DistributedMain <ZooKeeperQuorum> " +
        "<HDFSNamespace>");
    out.println(
      "Example: java -cp lib/*:$HBASE_CLASSPATH co.cask.tigon.DistributedMain 165.238.239.12:1234/tigon " +
        "tigon");
    out.println("");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  public static DistributedMain createDistributedMain(String zkString, String rootNamespace) throws IOException {
    return new DistributedMain(zkString, rootNamespace);
  }

  public static void main(String[] args) {
    System.out.println("Tigon Distributed Client");
    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      }

      if (args.length < 2) {
        usage(true);
      }

      String zkQuorumString = args[0];
      String rootNamespace = args[1];

      DistributedMain main = null;
      try {
        main = createDistributedMain(zkQuorumString, rootNamespace);
        main.startUp(System.out);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      } finally {
        try {
          if (main != null) {
            main.shutDown();
          }
          TerminalFactory.get().restore();
        } catch (Exception e) {
          LOG.warn(e.getMessage(), e);
        }
      }
    }
  }

  private void registerShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
        }
      }
    });
  }

  public void startUp(PrintStream out) throws Exception {
    registerShutDownHook();
    flowOperations.startAndWait();
    List<String> commandList = Lists.newArrayList();
    for (CLICommands cliCommand : CLICommands.values()) {
      commandList.add(cliCommand.toString().toLowerCase());
    }
    consoleReader.setPrompt("tigon> ");
    String line;
    while ((line = consoleReader.readLine()) != null) {
      String[] args = line.split("\\s+");
      String command = args[0].toUpperCase();
      try {
        CLICommands cmd = null;
        try {
          cmd = CLICommands.valueOf(command);
        } catch (IllegalArgumentException e) {
          out.println("Available Commands : ");
          out.println(StringUtils.join(commandList, ", "));
          continue;
        }

        if (args.length < cmd.getArgCount()) {
          throw new InvalidCLIArgumentException(cmd.printHelp());
        }

        if (cmd.equals(CLICommands.START)) {
          Map<String, String> runtimeArgs = Maps.newHashMap();
          if (args.length > cmd.getArgCount()) {
            try {
              runtimeArgs = DeployClient.fromPosixArray(Arrays.copyOfRange(args, cmd.getArgCount(), args.length));
            } catch (IllegalArgumentException e) {
              LOG.error("Runtime Args are not in the correct format [ --key1=val1 --key2=val2 ]");
              continue;
            }
          }
          flowOperations.startFlow(new File(args[1]), args[2], runtimeArgs);
        } else if (cmd.equals(CLICommands.LIST)) {
          out.println(StringUtils.join(flowOperations.listAllFlows(), ", "));
        } else if (cmd.equals(CLICommands.STOP)) {
          flowOperations.stopFlow(args[1]);
        } else if (cmd.equals(CLICommands.DELETE)) {
          flowOperations.deleteFlow(args[1]);
        } else if (cmd.equals(CLICommands.SET)) {
          flowOperations.setInstances(args[1], args[2], Integer.valueOf(args[3]));
        } else if (cmd.equals(CLICommands.STATUS)) {
          Service.State state = flowOperations.getStatus(args[1]);
          String status = (state != null) ? state.toString() : "NOT FOUND";
          out.println(status);
        } else if (cmd.equals(CLICommands.FLOWLETINFO)) {
          out.println(String.format("%-20s %s", "Flowlet Name", "Instance Count"));
          Map<String, Integer> flowletInfoMap = flowOperations.getFlowInfo(args[1]);
          for (Map.Entry<String, Integer> flowletInfo : flowletInfoMap.entrySet()) {
            out.println(String.format("%-20s %s", flowletInfo.getKey(), flowletInfo.getValue()));
          }
        } else if (cmd.equals(CLICommands.DISCOVER)) {
          for (InetSocketAddress socketAddress : flowOperations.discover(args[1], args[2])) {
            out.println(String.format("%s:%s", socketAddress.getHostName(), socketAddress.getPort()));
          }
        } else if (cmd.equals(CLICommands.SHOWLOGS)) {
          flowOperations.addLogHandler(args[1], System.out);
        } else if (cmd.equals(CLICommands.SERVICEINFO)) {
          out.println(StringUtils.join(flowOperations.getServices(args[1]), "\n"));
        } else if (cmd.equals(CLICommands.VERSION)) {
          out.println(Constants.VERSION);
        } else if (cmd.equals(CLICommands.HELP)) {
          try {
            out.println(CLICommands.valueOf(args[1].toUpperCase()).printHelp());
          } catch (IllegalArgumentException e) {
            out.println("Command Not Found");
          }
        } else {
          //QUIT Command
          break;
        }
      } catch (InvalidCLIArgumentException e) {
        out.println(e.getMessage());
      }
    }
  }

  public void shutDown() {
    try {
      flowOperations.stopAndWait();
      FileUtils.deleteDirectory(localDataDir);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private static List<Module> createModules(CConfiguration cConf, Configuration hConf) {
    return ImmutableList.of(
      new DataFabricDistributedModule(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new TwillModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new MetricsClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(FlowOperations.class).to(DistributedFlowOperations.class);
        }
      }
    );
  }

  private static final class MetricsClientModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }
}
