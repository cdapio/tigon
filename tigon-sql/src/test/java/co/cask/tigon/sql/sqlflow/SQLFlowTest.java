/**
 * Copyright 2012-2014 Continuuity, Inc.
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


package co.cask.tigon.sql.sqlflow;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.RoundRobin;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.flow.test.FlowManager;
import co.cask.tigon.flow.test.TestBase;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * SQLFlowTest
 */
public class SQLFlowTest extends TestBase {
  private static FlowManager flowManager;
  private static Thread ingestData;
  static final int MAX_TIMESTAMP = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    //Identifying free port
    final int port = getRandomPort();
    int tcpPort = getRandomPort();
    runtimeArgs.put(Constants.HTTP_PORT, Integer.toString(port));
    runtimeArgs.put(Constants.TCP_INGESTION_PORT_PREFIX + "intInput", Integer.toString(tcpPort));
    flowManager = deployFlow(SQLFlow.class, runtimeArgs);
    TimeUnit.SECONDS.sleep(15);
    ingestData = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 1; i <= MAX_TIMESTAMP; i++) {
            for (int j = 1; j <= i; j++) {
              HttpURLConnection urlConn =
                (HttpURLConnection) new URL("http://localhost:" + port + "/v1/tigon/intInput").openConnection();
              urlConn.setReadTimeout(2000);
              urlConn.setDoOutput(true);
              urlConn.setRequestProperty("Content-Type", "application/json");
              JsonObject bodyJson = new JsonObject();
              JsonArray dataArray = new JsonArray();
              dataArray.add(new JsonPrimitive(i));
              dataArray.add(new JsonPrimitive(j));
              bodyJson.add("data", dataArray);
              ByteStreams.copy(ByteStreams.newInputStreamSupplier(bodyJson.toString().
                getBytes(com.google.common.base.Charsets.UTF_8)), urlConn.getOutputStream());
              urlConn.getResponseCode();
            }
          }
        } catch (Exception e) {
          Throwables.propagate(e);
        }
      }
    });
  }

  @Test
  public void testSQLFlow() throws Exception {
    ingestData.start();
    ingestData.join();
    TimeUnit.SECONDS.sleep(15);
    int dataPacketCounter = MAX_TIMESTAMP;
    while (sharedDataQueue.size() > 0) {
      DataPacket dataPacket = sharedDataQueue.poll();
      Assert.assertEquals(evalSum(dataPacket.timestamp), dataPacket.sumValue);
      dataPacketCounter = dataPacketCounter - 1;
    }
    Assert.assertEquals(1, dataPacketCounter);
  }

  @AfterClass
  public static void afterClass() {
    flowManager.stop();
  }

  private int evalSum(long val) {
    // Sum of N natural numbers
    return (int) val * ((int) val + 1) / 2;
  }

  static Queue<DataPacket> sharedDataQueue = Queues.newConcurrentLinkedQueue();

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
        .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
        .addField("intStream", GDATFieldType.INT)
        .build();
      addJSONInput("intInput", schema);
      addQuery("sumOut", "SELECT timestamp, SUM(intStream) AS sumValue FROM intInput GROUP BY timestamp");
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

    @Override
    public void initialize(FlowletContext context) throws Exception {
      flowletName = context.getName() + "_" + context.getInstanceId();
    }

    @RoundRobin
    @ProcessInput
    public void process(DataPacket value) throws Exception {
      flowletLOG.info("{} got {}", flowletName, value.toString());
      sharedDataQueue.add(value);
    }
  }

  class DataPacket {
    //Using the same data type and variable name as specified in the query output
    long timestamp;
    int sumValue;

    public String toString() {
      return "timestamp : " + timestamp + "\tsumValue : " + sumValue;
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
}
