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
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.utils.Networks;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.gson.Gson;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * This is a base class that provides all the common functionality to test a Tigon SQL Flow.
 * The contract to be followed is that the last flowlet of the Flow that is being tested, pings output data packets to
 * an http end-point specified in the runtime args as "baseURL".
 */
public class SQLFlowTestBase extends TestBase {
  private static FlowManager flowManager;
  private static List<Thread> ingestionThreads = Lists.newArrayList();
  static CountDownLatch latch;
  private static TestHandler handler;
  private static NettyHttpService service;
  private static String serviceURL;
  private static final int httpPort = Networks.getRandomPort();
  private static final Gson GSON = new Gson();

  /**
   * This function deploys an instance of the flowClass.
   * Recommended, make this function call from a {@link org.junit.BeforeClass} annotated method.
   * @param flowClass Class of the {@link co.cask.tigon.api.flow.Flow} to be deployed
   * @throws Exception
   */
  public static void setupFlow(Class<? extends Flow> flowClass) throws Exception {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    handler = new TestHandler();
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(handler))
      .setPort(Networks.getRandomPort())
      .build();
    service.startAndWait();
    InetSocketAddress address = service.getBindAddress();
    serviceURL = "http://" + address.getHostName() + ":" + address.getPort() + "/queue";
    runtimeArgs.put("baseURL", serviceURL);
    runtimeArgs.put(Constants.HTTP_PORT, Integer.toString(httpPort));
    flowManager = deployFlow(flowClass, runtimeArgs);
    int max_wait = 100;
    // Waiting for the Tigon SQL Flow initialization
    while ((!flowManager.discover(Constants.HTTP_PORT).iterator().hasNext()) && (max_wait > 0)) {
      TimeUnit.SECONDS.sleep(1);
      max_wait = max_wait - 1;
    }
    if (max_wait <= 0) {
      throw new TimeoutException("Timeout Error, Tigon SQL flow took too long to initiate");
    }
  }

  /**
   * This function is used to add additional error checking by allowing the user to assert on the number of expected
   * output data packets.
   * @param count Number of expected output data packets
   * @return {@link java.util.concurrent.CountDownLatch} object that will count down to zero as output data packets are
   * received
   */
  public static CountDownLatch setExpectedOutputCount(int count) {
    latch = new CountDownLatch(count);
    return latch;
  }

  /**
   * This function is used to ingest data packets into multiple interfaces.
   * @param inputDataStreams {@link java.util.List} of {@link java.util.Map.Entry} that contains the name of the
   * interface as the key and the json encoded data packet as the value.
   *
   * Input Format :
   *            [ Map.Entry<InterfaceName, List<JsonEncodedDataPackets>>,
                  Map.Entry<InterfaceName, List<JsonEncodedDataPackets>>,
                  Map.Entry<InterfaceName, List<JsonEncodedDataPackets>>,
                  .
                  .
                  Map.Entry<InterfaceName, List<JsonEncodedDataPackets>>]
   */
  public static void ingestData(List<Map.Entry<String, List<String>>> inputDataStreams) {
    if (latch == null) {
      throw new RuntimeException("Expected output count not defined. Use setExpectedOutputCount()");
    } else if (service == null) {
      throw new RuntimeException("Flow not setup. Use setupFlow()");
    }
    final List<Map.Entry<String, List<String>>> final_inputDataStreams = inputDataStreams;
    Thread ingestData = new Thread(new Runnable() {
      @Override
      public void run() {
        HttpClient httpClient = new DefaultHttpClient();
        for (Map.Entry<String, List<String>> dataStreamEntry : final_inputDataStreams) {
          for (String dataPacket : dataStreamEntry.getValue()) {
            try {
              // TODO eliminate org.apache.http dependency TIGON-5
              HttpPost httpPost = new HttpPost("http://localhost:" + httpPort + "/v1/tigon/" + dataStreamEntry.getKey());
              httpPost.addHeader("Content-Type", "application/json");
              httpPost.setEntity(new StringEntity(dataPacket, Charsets.UTF_8));
              EntityUtils.consumeQuietly(httpClient.execute(httpPost).getEntity());
            } catch (Exception e) {
              Throwables.propagate(e);
            }
          }
        }
      }
    });
    ingestionThreads.add(ingestData);
    ingestData.start();
    try {
      ingestData.join(60000);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * This function is used to ingest data packets to a single interface.
   * @param inputDataStream {@link java.util.List} of {@link java.util.Map.Entry} that contains the name of the
   * interface as the key and the json encoded data packet as the value.
   *
   * Input Format :
   *            Map.Entry<InterfaceName, List<JsonEncodedDataPackets>>
   */
  public static void ingestData(Map.Entry<String, List<String>> inputDataStream) {
    List<Map.Entry<String, List<String>>> inputData = Lists.newArrayList();
    inputData.add(inputDataStream);
    ingestData(inputData);
  }

  public static final class TestHandler extends AbstractHttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TestHandler.class);
    private static Queue<String> queue = Queues.newConcurrentLinkedQueue();
    @Path("/queue/poll")
    @GET
    public void pollData(HttpRequest request, HttpResponder responder) {
      if (queue.size() == 0) {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
        return;
      }
      String dataPacket = queue.poll();
      responder.sendString(HttpResponseStatus.OK, dataPacket);
    }
    @Path("/queue")
    @POST
    public void getPing(HttpRequest request, HttpResponder responder) {
      String dataPacket = request.getContent().toString(Charsets.UTF_8);
      LOG.info("/ping Got Data {}", dataPacket);
      queue.add(dataPacket);
      latch.countDown();
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  /**
   * This function returns the output data packets generated by the given flow
   * @param outputClass {@link java.lang.Class} of the output type object
   * @param <T> The output type object class
   * @return An instance of the output type object
   * @throws IOException
   */
  public static <T> T getDataPacket(Class<T> outputClass) throws IOException {
    HttpClient httpClient = new DefaultHttpClient();
    HttpGet httpGet = new HttpGet(serviceURL + "/poll");
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      return null;
    }
    String serializedDataPacket = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
    return GSON.fromJson(serializedDataPacket, outputClass);
  }

  @AfterClass
  public static void afterClass() {
    flowManager.stop();
    service.stopAndWait();
  }
}
