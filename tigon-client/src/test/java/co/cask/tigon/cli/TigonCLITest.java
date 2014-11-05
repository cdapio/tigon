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

import co.cask.tigon.cli.commands.TigonCLI;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Tigon CLI Test.
 */
public class TigonCLITest {
  private static final String FLOWLET_NAME = "myflowlet";
  private static final int NUM_SERVICES = 2;
  private static final int SERVICE_PORT = 1234;
  private static final int DEBUG_PORT = 1000;

  private static class MockFlowOperations extends AbstractIdleService implements FlowOperations {
    private final List<String> flows;

    public MockFlowOperations() {
      flows = Lists.newArrayList();
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }

    @Override
    public void startFlow(File jarPath, String className, Map<String, String> userArgs, boolean debug) {
      flows.add(className);
    }

    @Override
    public State getStatus(String flowName) {
      if (flows.contains(flowName)) {
        return State.RUNNING;
      }
      return null;
    }

    @Override
    public List<String> listAllFlows() {
      return flows;
    }

    @Override
    public void stopFlow(String flowName) {
      flows.remove(flowName);
    }

    @Override
    public void deleteFlow(String flowName) {
      stopFlow(flowName);
    }

    @Override
    public List<String> getServices(String flowName) {
      List<String> services = Lists.newArrayList();
      for (int i = 0; i < NUM_SERVICES; i++) {
        services.add(String.format("%s.%s%d", flowName, "service", i));
      }
      return services;
    }

    @Override
    public List<InetSocketAddress> discover(String flowName, String service) {
      List<InetSocketAddress> addressList = Lists.newArrayList();
      for (int i = 0; i < NUM_SERVICES; i++) {
        addressList.add(new InetSocketAddress(SERVICE_PORT + i));
      }
      return addressList;
    }

    @Override
    public void setInstances(String flowName, String flowletName, int instanceCount) {

    }

    @Override
    public Map<String, Collection<TwillRunResources>> getFlowInfo(String flowName) {
      Map<String, Collection<TwillRunResources>> flowletInfo = Maps.newHashMap();
      List<TwillRunResources> resourcesList = Lists.newArrayList();
      resourcesList.add(new DefaultTwillRunResources(0, "container", 1, 1024, "localhost", DEBUG_PORT));
      flowletInfo.put(FLOWLET_NAME, resourcesList);
      return flowletInfo;
    }

    @Override
    public void addLogHandler(String flowName, PrintStream out) {

    }
  }

  @Test
  public void testCLI() throws Exception {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(FlowOperations.class).to(MockFlowOperations.class).in(Scopes.SINGLETON);
      }
    });

    TigonCLI tigonCLI = injector.getInstance(TigonCLI.class);
    testCommandOutputContains(tigonCLI, "status myflow", "NOT FOUND");
    testCommandOutputContains(tigonCLI, "start jarpath myflow", null);
    testCommandOutputContains(tigonCLI, "list", "myflow");
    testCommandOutputContains(tigonCLI, "status myflow", "RUNNING");

    testCommandOutputContains(tigonCLI, "serviceinfo myflow", "myflow.service0");
    testCommandOutputContains(tigonCLI, "discover myflow.myservice", Integer.toString(SERVICE_PORT));

    testCommandOutputContains(tigonCLI, "flowletinfo myflow", FLOWLET_NAME);
    testCommandOutputContains(tigonCLI, "debuginfo myflow." + FLOWLET_NAME, Integer.toString(DEBUG_PORT));

    //No-op but makes sure set, showlogs commands do not throw an error for the command patterns
    testCommandOutputContains(tigonCLI, "set myflow.dummyflowlet 2", null);
    testCommandOutputContains(tigonCLI, "showlogs myflow", null);

    testCommandOutputContains(tigonCLI, "delete myflow", null);
    testCommandOutputContains(tigonCLI, "status myflow", "NOT FOUND");
    testCommandOutputContains(tigonCLI, "debug jarpath newflow", null);
    testCommandOutputContains(tigonCLI, "list", "newflow");
    testCommandOutputContains(tigonCLI, "stop newflow", null);
  }

  private void testCommandOutputContains(TigonCLI cli, String command, final String expectedOutput) throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        if (expectedOutput != null) {
          Assert.assertTrue(String.format("Expected output '%s' to contain '%s'", output, expectedOutput),
                            output != null && output.contains(expectedOutput));
        }
        return null;
      }
    });
  }

  private void testCommand(TigonCLI cli, String command, Function<String, Void> outputValidator) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    cli.execute(command, printStream);
    String output = outputStream.toString();
    outputValidator.apply(output);
  }
}
