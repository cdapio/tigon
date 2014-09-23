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

package co.cask.tigon.flow.test;

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.tigon.api.annotation.HashPartition;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.FlowletSpecification;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Basic integration tests for Flows.
 *
 * The testing is done via a NettyHttp service. Different requests are made to the service depending on actions/state,
 * and these actions/states are tested by querying the service.
 */
public class BasicFlowTest extends TestBase {

  private static final String GENERATOR_FLOWLET_ID = "generator";
  private static final String SINK_FLOWLET_ID = "sink";

  private static NettyHttpService service;
  private static String baseURL;
  private static FlowManager flowManager;
  private static HttpClient httpClient;

  // Endpoints exposed by the Netty service.
  private static final class EndPoints {
    private static final String INSTANCES = "/instances/{key}";
    private static final String PING = "/ping";
    private static final String COUNTDOWN = "/countdown";
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Create and start the Netty service.
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(new TestHandler()))
      .build();

    service.startAndWait();
    InetSocketAddress address = service.getBindAddress();
    baseURL = "http://" + address.getHostName() + ":" + address.getPort();

    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("baseURL", baseURL);

    httpClient = new HttpClient();
    // Start the flow and let it run for 15 seconds.
    flowManager = deployFlow(TestFlow.class, runtimeArgs);
    TimeUnit.SECONDS.sleep(15);
  }

  @AfterClass
  public static void afterClass() {
    flowManager.stop();
    service.stopAndWait();
  }

  @Test
  public void testFlow() throws Exception {
    // Exactly 10 events should have been emitted from the generator to the sink.
    GetMethod method = new GetMethod(baseURL + EndPoints.PING);
    httpClient.executeMethod(method);
    int pingCount = Integer.valueOf(method.getResponseBodyAsString());
    Assert.assertEquals(10, pingCount);
  }

  @Test
  public void testInstanceChange() throws Exception {
    flowManager.setFlowletInstances(GENERATOR_FLOWLET_ID, 5);
    TimeUnit.SECONDS.sleep(5);

    GetMethod generatorInstancesMethod = new GetMethod(baseURL + getFlowletInstancesEndpoint(GENERATOR_FLOWLET_ID));
    httpClient.executeMethod(generatorInstancesMethod);
    int flowletInstances = Integer.valueOf(generatorInstancesMethod.getResponseBodyAsString());
    Assert.assertEquals(5, flowletInstances);

    flowManager.setFlowletInstances(GENERATOR_FLOWLET_ID, 2);
    TimeUnit.SECONDS.sleep(5);

    httpClient.executeMethod(generatorInstancesMethod);
    flowletInstances = Integer.valueOf(generatorInstancesMethod.getResponseBodyAsString());
    Assert.assertEquals(2, flowletInstances);

    flowManager.setFlowletInstances(SINK_FLOWLET_ID, 3);
    TimeUnit.SECONDS.sleep(5);

    GetMethod sinkInstancesMethod = new GetMethod(baseURL + getFlowletInstancesEndpoint(SINK_FLOWLET_ID));
    httpClient.executeMethod(sinkInstancesMethod);
    int sinkInstances = Integer.valueOf(sinkInstancesMethod.getResponseBodyAsString());
    Assert.assertEquals(3, sinkInstances);

    // The sink flowlet is configured to have a maximum of only 3 instances.
    // Setting a number above that should fail, and the instance count should remain the same.
    flowManager.setFlowletInstances(SINK_FLOWLET_ID, 5);
    TimeUnit.SECONDS.sleep(5);

    httpClient.executeMethod(sinkInstancesMethod);
    sinkInstances = Integer.valueOf(sinkInstancesMethod.getResponseBodyAsString());
    Assert.assertEquals(3, sinkInstances);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConfigurationFlow() {
    // The Flow is configured to have 5 instances of the Sink flowlet, which allows a maximum of 3 instances.
    // It should fail at deploy.
    deployFlow(InvalidConfigurationFlow.class, Maps.<String, String>newHashMap());
  }

  public static final class TestFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("testFlow")
        .setDescription("")
        .withFlowlets()
        .add(GENERATOR_FLOWLET_ID, new GeneratorFlowlet(), 1)
        .add(SINK_FLOWLET_ID, new SinkFlowlet(), 1)
        .connect()
        .from(GENERATOR_FLOWLET_ID).to(SINK_FLOWLET_ID)
        .build();
    }
  }

  public static final class InvalidConfigurationFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("invalidTestFlow")
        .setDescription("")
        .withFlowlets()
        .add(GENERATOR_FLOWLET_ID, new GeneratorFlowlet(), 1)
        .add(SINK_FLOWLET_ID, new SinkFlowlet(), 5)
        .connect()
        .from(GENERATOR_FLOWLET_ID).to(SINK_FLOWLET_ID)
        .build();
    }
  }

  private static final class GeneratorFlowlet extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorFlowlet.class);
    private OutputEmitter<Integer> intEmitter;
    private int i = 0;
    private HttpClient client;
    private HttpMethod countdownMethod;
    private HttpMethod decreaseInstanceMethod;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      String baseURL = context.getRuntimeArguments().get("baseURL");
      client = new HttpClient();

      LOG.info("Starting GeneratorFlowlet.");

      String generatorInstancesEndpoint = baseURL + getFlowletInstancesEndpoint(GENERATOR_FLOWLET_ID);
      // Notify the NettyServer that a new Generator flowlet is initialized.
      HttpMethod increaseInstanceMethod = new PostMethod(generatorInstancesEndpoint);
      client.executeMethod(increaseInstanceMethod);

      countdownMethod = new GetMethod(baseURL + EndPoints.COUNTDOWN);
      decreaseInstanceMethod = new DeleteMethod(generatorInstancesEndpoint);
    }

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    @SuppressWarnings("UnusedDeclaration")
    public void process() throws Exception {
      // Emit an event only if 10 events haven't been emitted yet.
      if (client.executeMethod(countdownMethod) == 200) {
        Integer value = ++i;
        intEmitter.emit(value, "integer", value.hashCode());
        LOG.info("Sending data {} to sink.", value);
      }
    }

    @Override
    public void destroy() {
      try {
        client.executeMethod(decreaseInstanceMethod);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static final class SinkFlowlet extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFlowlet.class);
    private HttpClient client;
    private HttpMethod pingMethod;

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with()
        .setName(getName())
        .setDescription(getDescription())
        .setMaxInstances(3)
        .build();
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      String baseURL = context.getRuntimeArguments().get("baseURL");
      client = new HttpClient();
      pingMethod = new PostMethod(baseURL + EndPoints.PING);

      LOG.info("Starting SinkFlowlet.");

      // Notify the server that a new Sink flowlet is initialized.
      HttpMethod increaseInstanceMethod = new PostMethod(baseURL + getFlowletInstancesEndpoint(SINK_FLOWLET_ID));
      client.executeMethod(increaseInstanceMethod);
    }

    @HashPartition("integer")
    @ProcessInput
    @SuppressWarnings("UnusedDeclaration")
    public void process(Integer value) throws Exception {
      // Notify the server that a an event has been received.
      client.executeMethod(pingMethod);
      LOG.info("Ping NettyService.");
    }
  }

  public static final class TestHandler extends AbstractHttpHandler {
    private static AtomicInteger countdownLimit = new AtomicInteger(10);
    private static AtomicInteger pingCount = new AtomicInteger(0);
    private static Multiset<String> flowletInstanceCounts = ConcurrentHashMultiset.create();

    @Path(EndPoints.PING)
    @POST
    public void incrementPing(HttpRequest request, HttpResponder responder) {
      pingCount.incrementAndGet();
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.PING)
    @GET
    public void getPingCount(HttpRequest request, HttpResponder responder) {
      responder.sendJson(HttpResponseStatus.OK, pingCount.get());
    }

    @Path(EndPoints.COUNTDOWN)
    @GET
    public void countdown(HttpRequest request, HttpResponder responder) {
      if (countdownLimit.intValue() > 0) {
        countdownLimit.decrementAndGet();
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
      }
    }

    @Path(EndPoints.INSTANCES)
    @POST
    public void increaseInstanceCount(HttpRequest request, HttpResponder responder, @PathParam("key") String key) {
      flowletInstanceCounts.add(key);
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.INSTANCES)
    @DELETE
    public void decreaseInstanceCount(HttpRequest request, HttpResponder responder, @PathParam("key") String key) {
      flowletInstanceCounts.remove(key);
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.INSTANCES)
    @GET
    public void getInstanceCount(HttpRequest request, HttpResponder responder, @PathParam("key") String key) {
      responder.sendJson(HttpResponseStatus.OK, flowletInstanceCounts.count(key));
    }
  }

  protected static String getFlowletInstancesEndpoint(String flowletId) {
    return EndPoints.INSTANCES.replace("{key}", flowletId);
  }
}
