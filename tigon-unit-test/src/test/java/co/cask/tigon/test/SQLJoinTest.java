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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * SQLJoinTest
 */
public class SQLJoinTest extends SQLFlowTestBase {
  private static final int MAX = 10;
  private static CountDownLatch latch;

  /**
   * Setup and deploy {@link co.cask.tigon.test.SQLTest.SQLFlow}
   * @throws Exception
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    setupFlow(SQLFlow.class);
    latch = setExpectedOutputCount(MAX);
  }

  /**
   * Run test
   * @throws Exception
   */
  @Test
  public void testSQLFlow() throws Exception {
    //Generating input data packets
    List<String> nameData = Lists.newArrayList();
    for (int i = 1; i <= MAX; i++) {
      JsonObject bodyJson = new JsonObject();
      JsonArray dataArray = new JsonArray();
      dataArray.add(new JsonPrimitive(Integer.toString(i)));
      dataArray.add(new JsonPrimitive("NAME" + i));
      bodyJson.add("data", dataArray);
      nameData.add(bodyJson.toString());
    }
    List<String> ageData = Lists.newArrayList();
    for (int i = 1; i <= MAX; i++) {
      JsonObject bodyJson = new JsonObject();
      JsonArray dataArray = new JsonArray();
      dataArray.add(new JsonPrimitive(Integer.toString(i)));
      dataArray.add(new JsonPrimitive(i * 10));
      bodyJson.add("data", dataArray);
      ageData.add(bodyJson.toString());
    }
    List<Map.Entry<String, List<String>>> dataStreams = Lists.newArrayList();
    dataStreams.add(new AbstractMap.SimpleEntry<String, List<String>>("nameInput", nameData));
    dataStreams.add(new AbstractMap.SimpleEntry<String, List<String>>("ageInput", ageData));
    ingestData(dataStreams);
    latch.await(60, TimeUnit.SECONDS);
    int dataPacketCounter = MAX;
    DataPacket dataPacket;
    while ((dataPacket = getDataPacket(DataPacket.class)) != null) {
      Assert.assertEquals(dataPacket.uid * 10, dataPacket.age);
      Assert.assertEquals("NAME" + dataPacket.uid, dataPacket.name);
      dataPacketCounter = dataPacketCounter - 1;
    }
    Assert.assertEquals(0, dataPacketCounter);
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
    int uid;
    String name;
    int age;

    public String toString() {
      return "UID: " + uid + "\tName: " + name + "\tAge: " + age;
    }
  }

  private static final class SQLFlowlet extends AbstractInputFlowlet {
    private OutputEmitter<DataPacket> dataEmitter;
    private final Logger flowletLOG = LoggerFactory.getLogger(SQLFlowlet.class);

    @Override
    public void create() {
      setName("Joining");
      setDescription("Join two Streams");
      StreamSchema ageSchema = new StreamSchema.Builder()
        .setName("ageData")
        .addField("uid", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
        .addField("age", GDATFieldType.INT)
        .build();
      StreamSchema nameSchema = new StreamSchema.Builder()
        .setName("nameData")
        .addField("uid", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
        .addField("name", GDATFieldType.STRING)
        .build();
      addJSONInput("ageInput", ageSchema);
      addJSONInput("nameInput", nameSchema);

      addQuery("userDetails", "SELECT nI.uid as uid, nI.name as name, aI.age as age INNER_JOIN " +
        "FROM nameInput.nameData nI, ageInput.ageData aI WHERE nI.uid = aI.uid");
    }

    @QueryOutput("userDetails")
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
        bodyJson.addProperty("uid", value.uid);
        bodyJson.addProperty("name", value.name);
        bodyJson.addProperty("age", value.age);
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
