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
import co.cask.tigon.cli.DistributedFlowOperations;
import co.cask.tigon.cli.FlowOperations;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricDistributedModule;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.guice.TwillModule;
import co.cask.tigon.guice.ZKClientModule;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
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
import java.util.List;

/**
 * Tigon Distributed Main.
 */
public class DistributedMain {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedMain.class);

  private final File localDataDir;
  private final FlowOperations flowOperations;
  private final ConsoleReader consoleReader;
  private enum Commands {
    START, LIST, STOP, DELETE, DISCOVER, SET, GETINFO, SHOWLOGS
  }

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
    consoleReader.setPrompt("tigon> ");
    String line;
    while ((line = consoleReader.readLine()) != null) {
      String[] args = line.split(" ");
      String command = args[0].toUpperCase();
      if (command.equals(Commands.START.toString())) {
        Preconditions.checkArgument(args.length == 3);
        flowOperations.startFlow(new File(args[1]), args[2]);
      } else if (command.equals(Commands.LIST.toString())) {
        out.println(StringUtils.join(flowOperations.listAllFlows(), ", "));
      } else if (command.equals(Commands.STOP.toString())) {
        Preconditions.checkArgument(args.length == 2);
        flowOperations.stopFlow(args[1]);
      } else if (command.equals(Commands.DELETE.toString())) {
        Preconditions.checkArgument(args.length == 2);
        flowOperations.deleteFlow(args[1]);
      } else if (command.equals(Commands.SET.toString())) {
        Preconditions.checkArgument(args.length == 4);
        flowOperations.setInstances(args[1], args[2], Integer.valueOf(args[3]));
      } else if (command.equals(Commands.GETINFO.toString())) {
        Preconditions.checkArgument(args.length == 2);
        out.println(StringUtils.join(flowOperations.getFlowInfo(args[1]), "\n"));
      } else if (command.equals(Commands.DISCOVER.toString())) {
        Preconditions.checkArgument(args.length == 3);
        List<String> endpoints = Lists.newArrayList();
        for (InetSocketAddress address : flowOperations.discover(args[1], args[2])) {
          endpoints.add(address.getHostName() + ":" + address.getPort());
        }
        out.println(StringUtils.join(endpoints, "\n"));
      } else if (command.equals(Commands.SHOWLOGS.toString())) {
        Preconditions.checkArgument(args.length == 2);
        flowOperations.addLogHandler(args[1], System.out);
      } else {
        out.println("Available Commands : ");
        for (Commands cmd : Commands.values()) {
          out.println(cmd.toString());
        }
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
