/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.http.HttpRequest;
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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Basic integration tests for Flows.
 *
 * The testing is done via a NettyHttp service. Different requests are made to the service depending on actions/state,
 * and these actions/states are tested by querying the service.
 */
public class BasicFlowTest extends TestBase {

  private static NettyHttpService service;
  private static String baseURL;
  private static FlowManager flowManager;
  private static HttpClient httpClient;

  // Endpoints exposed by the Netty service.
  private static final class EndPoints {
    private static final String GENERATOR_INSTANCES = "/instances";
    private static final String PING = "/ping";
    private static final String COUNTDOWN = "/countdown";
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(new TestHandler()))
      .setExecThreadPoolSize(1)
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
    GetMethod method = new GetMethod(baseURL + EndPoints.PING);
    httpClient.executeMethod(method);
    int pingCount = Integer.valueOf(method.getResponseBodyAsString());
    Assert.assertEquals(10, pingCount);
  }

  @Test
  public void testInstanceChange() throws Exception {
    flowManager.setFlowletInstances("generator", 5);
    TimeUnit.SECONDS.sleep(5);

    GetMethod method = new GetMethod(baseURL + EndPoints.GENERATOR_INSTANCES);
    httpClient.executeMethod(method);
    int instanceCount = Integer.valueOf(method.getResponseBodyAsString());
    Assert.assertEquals(5, instanceCount);

    flowManager.setFlowletInstances("generator", 2);
    TimeUnit.SECONDS.sleep(5);

    httpClient.executeMethod(method);
    instanceCount = Integer.valueOf(method.getResponseBodyAsString());
    Assert.assertEquals(2, instanceCount);
  }

  public static final class TestFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("testFlow")
        .setDescription("")
        .withFlowlets()
        .add("generator", new GeneratorFlowlet(), 1)
        .add("sink", new SinkFlowlet(), 1)
        .connect()
        .from("generator").to("sink")
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

    public void initialize(FlowletContext context) throws Exception {
      String baseURL = context.getRuntimeArguments().get("baseURL");
      client = new HttpClient();

      LOG.info("Starting GeneratorFlowlet.");

      // Notify the NettyServer that a new Generator flowlet is initialized.
      HttpMethod increaseInstanceMethod = new PostMethod(baseURL + EndPoints.GENERATOR_INSTANCES);
      client.executeMethod(increaseInstanceMethod);

      countdownMethod = new GetMethod(baseURL + EndPoints.COUNTDOWN);
      decreaseInstanceMethod = new DeleteMethod(baseURL + EndPoints.GENERATOR_INSTANCES);
    }

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void process() throws Exception {
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
    public void initialize(FlowletContext context) throws Exception {
      String baseURL = context.getRuntimeArguments().get("baseURL");
      client = new HttpClient();
      pingMethod = new PostMethod(baseURL + EndPoints.PING);
      LOG.info("Starting SinkFlowlet.");
    }

    @HashPartition("integer")
    @ProcessInput
    public void process(Integer value) throws Exception {
      client.executeMethod(pingMethod);
      LOG.info("Ping NettyService.");
    }
  }

  public static final class TestHandler extends AbstractHttpHandler {
    private static int countdownLimit = 10;
    private static int pingCount = 0;
    private static int instanceCount = 0;

    @Path(EndPoints.PING)
    @POST
    public void incrementPing(HttpRequest request, HttpResponder responder) {
      pingCount++;
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.PING)
    @GET
    public void getPingCount(HttpRequest request, HttpResponder responder) {
      responder.sendJson(HttpResponseStatus.OK, pingCount);
    }

    @Path(EndPoints.COUNTDOWN)
    @GET
    public void countdown(HttpRequest request, HttpResponder responder) {
      if (countdownLimit > 0) {
        countdownLimit--;
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
      }
    }

    @Path(EndPoints.GENERATOR_INSTANCES)
    @POST
    public void increaseInstanceCount(HttpRequest request, HttpResponder responder) {
      instanceCount++;
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.GENERATOR_INSTANCES)
    @DELETE
    public void decreaseInstanceCount(HttpRequest request, HttpResponder responder) {
      instanceCount--;
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path(EndPoints.GENERATOR_INSTANCES)
    @GET
    public void getInstanceCount(HttpRequest request, HttpResponder responder) {
      responder.sendJson(HttpResponseStatus.OK, instanceCount);
    }

  }
}
