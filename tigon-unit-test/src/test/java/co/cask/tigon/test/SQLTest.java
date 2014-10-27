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

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.RoundRobin;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * SQLTest
 */
public class SQLTest extends SQLFlowTestBase {
  private static final int MAX = 10;
  private static CountDownLatch latch;

  /**
   * Setup and deploy {@link co.cask.tigon.test.SQLTest.SQLFlow}
   * @throws Exception
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    setupFlow(SQLFlow.class);
    latch = setExpectedOutputCount(MAX - 1);
  }

  /**
   * Run test
   * @throws Exception
   */
  @Test
  public void testSQLFlow() throws Exception {
    List<String> inputDataList = Lists.newArrayList();
    for (int i = 1; i <= MAX; i++) {
      for (int j = 1; j <= i; j++) {
        JsonObject bodyJson = new JsonObject();
        JsonArray dataArray = new JsonArray();
        dataArray.add(new JsonPrimitive(Integer.toString(i)));
        dataArray.add(new JsonPrimitive(Integer.toString(j)));
        bodyJson.add("data", dataArray);
        inputDataList.add(bodyJson.toString());
      }
    }
    ingestData(new AbstractMap.SimpleEntry<String, List<String>>("inputDataStream", inputDataList));
    latch.await(60, TimeUnit.SECONDS);
    int dataPacketCounter = MAX;
    DataPacket dataPacket;
    while ((dataPacket = getDataPacket(DataPacket.class)) != null) {
      Assert.assertEquals(evalSum(dataPacket.timestamp), dataPacket.sumValue);
      dataPacketCounter = dataPacketCounter - 1;
    }
    // Expected output count is MAX - 1 as the last timestamp value is not processed
    Assert.assertEquals(1, dataPacketCounter);
  }

  private static int evalSum(long val) {
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

  private static class DataPacket {
    //Using the same data type and variable name as specified in the query output
    long timestamp;
    int sumValue;

    public String toString() {
      return "timestamp : " + timestamp + "\tsumValue : " + sumValue;
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
      addJSONInput("inputDataStream", schema);
      addQuery("sumOut",
               "SELECT timestamp, SUM(intStream) AS sumValue FROM inputDataStream.intInput GROUP BY timestamp");
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
        bodyJson.addProperty("timestamp", value.timestamp);
        bodyJson.addProperty("sumValue", value.sumValue);
        HttpPost httpPost = new HttpPost(baseURL);
        StringEntity params = new StringEntity(bodyJson.toString(), Charsets.UTF_8);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(params);
        EntityUtils.consumeQuietly(httpClient.execute(httpPost).getEntity());
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
