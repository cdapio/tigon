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

package co.cask.tigon.test;

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.RoundRobin;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
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
import java.net.ServerSocket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * SQLFlowTest
 */
public class SQLFlowTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static FlowManager flowManager;
  private static Thread ingestData;
  static final int MAX_TIMESTAMP = 10;
  static CountDownLatch latch;
  private static TestHandler handler;
  private static NettyHttpService service;
  private static String serviceURL;

  @BeforeClass
  public static void beforeClass() throws Exception {
    latch = new CountDownLatch(MAX_TIMESTAMP);
    Map<String, String> runtimeArgs = Maps.newHashMap();
    //Identifying free port
    handler = new TestHandler();
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(handler))
      .setPort(getRandomPort())
      .build();
    service.startAndWait();
    InetSocketAddress address = service.getBindAddress();
    serviceURL = "http://" + address.getHostName() + ":" + address.getPort();
    runtimeArgs.put("baseURL", serviceURL);
    final int port = getRandomPort();
    int tcpPort = getRandomPort();
    runtimeArgs.put(Constants.HTTP_PORT, Integer.toString(port));
    runtimeArgs.put(Constants.TCP_INGESTION_PORT_PREFIX + "inputInterface", Integer.toString(tcpPort));
    flowManager = deployFlow(SQLFlow.class, runtimeArgs);
    TimeUnit.SECONDS.sleep(60);
    ingestData = new Thread(new Runnable() {
      @Override
      public void run() {
        HttpClient httpClient = new DefaultHttpClient();
        for (int i = 1; i <= MAX_TIMESTAMP; i++) {
          for (int j = 1; j <= i; j++) {
            try {
              // TODO eliminate org.apache.http dependency TIGON-5
              HttpPost httpPost = new HttpPost("http://localhost:" + port + "/v1/tigon/inputInterface");
              JsonObject bodyJson = new JsonObject();
              JsonArray dataArray = new JsonArray();
              dataArray.add(new JsonPrimitive(Integer.toString(i)));
              dataArray.add(new JsonPrimitive(Integer.toString(j)));
              bodyJson.add("data", dataArray);
              StringEntity params = new StringEntity(bodyJson.toString(), Charsets.UTF_8);
              httpPost.addHeader("Content-Type", "application/json");
              httpPost.setEntity(params);
              EntityUtils.consumeQuietly(httpClient.execute(httpPost).getEntity());
            } catch (Exception e) {
              Throwables.propagate(e);
            }
          }
        }
        latch.countDown();
      }
    });
  }

  private static DataPacket getDataPacket() throws IOException {
    HttpClient httpClient = new DefaultHttpClient();
    HttpGet httpGet = new HttpGet(serviceURL + "/poll-data");
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      return null;
    }
    String serializedDataPacket = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
    return GSON.fromJson(serializedDataPacket, DataPacket.class);
  }

  @Test
  public void testSQLFlow() throws Exception {
    ingestData.start();
    ingestData.join();
    latch.await(60, TimeUnit.SECONDS);
    int dataPacketCounter = MAX_TIMESTAMP;
    DataPacket dataPacket;
    while ((dataPacket = getDataPacket()) != null) {
      Assert.assertEquals(evalSum(dataPacket.timestamp), dataPacket.sumValue);
      dataPacketCounter = dataPacketCounter - 1;
    }
    Assert.assertEquals(1, dataPacketCounter);
  }

  @AfterClass
  public static void afterClass() {
    flowManager.stop();
    service.stopAndWait();
  }

  private int evalSum(long val) {
    // Sum of N natural numbers
    return (int) val * ((int) val + 1) / 2;
  }


  public static final class SQLFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SQLFlow")
        .setDescription("Sample Flow")
        .withFlowlets()
        .add("SQLProcessor", new SQLFlowlet(), 1)
        .add("dataSink", new SinkFlowlet(), 3)
        .connect()
        .from("SQLProcessor").to("dataSink")
        .build();
    }
  }

  private static final class SQLFlowlet extends AbstractInputFlowlet {
    private OutputEmitter<DataPacket> dataEmitter;
    private final Logger flowletLOG = LoggerFactory.getLogger(SQLFlowlet.class);

    @Override
    public void create() {
      setName("Summation");
      setDescription("sums up the input value over a timewindow");
      StreamSchema schema = new StreamSchema.Builder()
        .setName("intInput")
        .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
        .addField("intStream", GDATFieldType.INT)
        .build();
      addJSONInput("inputInterface", schema);
      addQuery("sumOut",
               "SELECT timestamp, SUM(intStream) AS sumValue FROM inputInterface.intInput GROUP BY timestamp");
    }

    @QueryOutput("sumOut")
    public void emitData(DataPacket dataPacket) {
      flowletLOG.info("Emitting data to next flowlet");
      //Each data packet is forwarded to the next flowlet
      dataEmitter.emit(dataPacket);
    }
  }

  private static final class SinkFlowlet extends AbstractFlowlet {
    private final Logger flowletLOG = LoggerFactory.getLogger(SinkFlowlet.class);
    private String flowletName;
    private String baseURL;
    // TODO eliminate org.apache.http dependency TIGON-5
    private HttpClient httpClient;


    @Override
    public void initialize(FlowletContext context) throws Exception {
      flowletName = context.getName() + "_" + context.getInstanceId();
      baseURL = context.getRuntimeArguments().get("baseURL");
      httpClient = new DefaultHttpClient();
    }

    @RoundRobin
    @ProcessInput
    public void process(DataPacket value) throws Exception {
      flowletLOG.info("{} got {}", flowletName, value.toString());
      try {
        JsonObject bodyJson = new JsonObject();
        bodyJson.addProperty("time", value.getTime());
        bodyJson.addProperty("sum", value.getSum());
        HttpPost httpPost = new HttpPost(baseURL + "/ping");
        StringEntity params = new StringEntity(bodyJson.toString(), Charsets.UTF_8);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(params);
        EntityUtils.consumeQuietly(httpClient.execute(httpPost).getEntity());
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  public static int getRandomPort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      return -1;
    }
  }

  public static final class TestHandler extends AbstractHttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TestHandler.class);
    private static JsonObject requestData;
    private static Queue<DataPacket> queue = Queues.newConcurrentLinkedQueue();

    @Path("/poll-data")
    @GET
    public void getDataPacket(HttpRequest request, HttpResponder responder) {
      if (queue.size() == 0) {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
        return;
      }
      DataPacket dataPacket = queue.poll();
      responder.sendJson(HttpResponseStatus.OK, dataPacket);
    }


    @Path("/ping")
    @POST
    public void getPing(HttpRequest request, HttpResponder responder) {
      try {
        requestData = GSON.fromJson(request.getContent().toString(Charsets.UTF_8), JsonObject.class);
      } catch (Exception e) {
        throw new RuntimeException("Cannot parse JSON data from the HTTP response");
      }
      DataPacket dataPacket = new DataPacket(requestData.get("time").getAsLong(), requestData.get("sum").getAsInt());
      LOG.info("/ping Got Data {}", dataPacket.toString());
      queue.add(dataPacket);
      latch.countDown();
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  private static class DataPacket {
    //Using the same data type and variable name as specified in the query output
    long timestamp;
    int sumValue;

    public String toString() {
      return "timestamp : " + timestamp + "\tsumValue : " + sumValue;
    }

    public DataPacket(long timestamp, int sumValue) {
      this.timestamp = timestamp;
      this.sumValue = sumValue;
    }

    public String getTime() {
      return Long.toString(timestamp);
    }

    public String getSum() {
      return Integer.toString(sumValue);
    }
  }
}
