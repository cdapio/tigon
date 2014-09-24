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

package co.cask.tigon.sql.manager;

import co.cask.tigon.api.metrics.Metrics;
import co.cask.tigon.sql.internal.HealthInspector;
import co.cask.tigon.sql.internal.MetricsRecorder;
import co.cask.tigon.sql.internal.ProcessMonitor;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonObject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;

/**
 * DiscoveryServerTest
 */

public class DiscoveryServerTest {
  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryServerTest.class);
  private static URI baseURI;
  private static DiscoveryServer discoveryServer;

  @BeforeClass
  public static void setup() throws Exception {
    List<HubDataSource> sourceList = Lists.newArrayList();
    sourceList.add(new HubDataSource("source1", new InetSocketAddress("127.0.0.1", 8081)));
    sourceList.add(new HubDataSource("source2", new InetSocketAddress("127.0.0.1", 8082)));
    List<HubDataSink> sinkList = Lists.newArrayList();
    sinkList.add(new HubDataSink("sink1", "query1", new InetSocketAddress("127.0.0.1", 7081)));
    sinkList.add(new HubDataSink("sink2", "query2", new InetSocketAddress("127.0.0.1", 7082)));
    HubDataStore hubDataStore = new HubDataStore.Builder()
      .setInstanceName("test")
      .setDataSource(sourceList)
      .setDataSink(sinkList)
      .setHFTACount(5)
      .setClearingHouseAddress(new InetSocketAddress("127.0.0.1", 1111))
      .build();
    HealthInspector inspector = new HealthInspector(new ProcessMonitor() {
      @Override
      public void notifyFailure(Set<String> errorProcessNames) {
        LOG.info("Heartbeat detection failure notified");
        for (String error : errorProcessNames) {
          LOG.error("No Heartbeat received from " + error);
        }
      }

      @Override
      public void announceReady() {
        //no-op
      }
    });
    MetricsRecorder metricsRecorder = new MetricsRecorder(new Metrics() {
      @Override
      public void count(String counterName, int delta) {
        LOG.info("[METRICS] CounterName : {}\tValue last second : {}", counterName, delta);
      }
    });
    discoveryServer = new DiscoveryServer(hubDataStore, inspector, metricsRecorder, null);
    discoveryServer.startAndWait();
    baseURI = URI.create(String.format("http://" + discoveryServer.getHubAddress().getAddress().getHostAddress() +
                                         ":" + discoveryServer.getHubAddress().getPort()));
  }

  private HttpURLConnection request(String path, HttpMethod method) throws IOException {
    URL url = baseURI.resolve(path).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      urlConn.setDoOutput(true);
    }
    urlConn.setRequestMethod(method.getName());
    urlConn.setRequestProperty(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
    return urlConn;
  }

  private String getContent(HttpURLConnection urlConn) throws IOException {
    return new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8);
  }

  private void writeContent(HttpURLConnection urlConn, String content) throws IOException {
    urlConn.getOutputStream().write(content.getBytes(Charsets.UTF_8));
  }

  private String createJSONString(String name, String ip, int port, String processID, JsonObject metrics,
                                  String processName) {
    JsonObject res = new JsonObject();
    if (name != null) {
      res.addProperty("name", name);
    }
    if (ip != null) {
      res.addProperty("ip", ip);
    }
    if (port > -1) {
      res.addProperty("port", port);
    }
    if (processName != null) {
      res.addProperty("fta_name", processID);
    }
    if (processID != null) {
      res.addProperty("ftaid", processID);
    }
    if (metrics != null) {
      res.add("metrics", metrics);
    }
    return res.toString();
  }

  private String createJSONString(String name, String ip, int port, String processID, JsonObject metrics) {
    return createJSONString(name, ip, port, processID, metrics, null);
  }

  private String createJSONString(String name, String ip, int port) {
    return createJSONString(name, ip, port, null, null, null);
  }

  @Test
  public void testServiceState() throws IOException {
    Assert.assertEquals(Service.State.RUNNING, discoveryServer.state());
  }

  @Test
  public void testServices() throws IOException {
    //Test Announce Instance
    badAnnounceInitializedInstance();
    announceInstance();
    //Test Discover Instance
    badDiscoverInstance();
    discoverInstance();
    //Test Discover Initialized Instance
    badDiscoverInitializedInstance1();
    announceInitializedInstance();
    badDiscoverInitializedInstance();
    discoverInitializedInstance();
    //Test Discover Start Processing
    earlyDiscoverStartProcessing();
    announceStreamSubscription();
    earlyDiscoverStartProcessing();
    badDiscoverStartProcessing();
    announceStreamSubscription();
    discoverStartProcessing();
    announceMetrics();
    logMetrics();
  }

  @Test
  public void testDatSource() throws IOException {
    dataSource();
    badDataSource();
  }

  @Test
  public void testDataSink() throws IOException {
    dataSink();
    badDataSink();
  }

  private void badAnnounceInitializedInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/announce-initialized-instance", HttpMethod.POST);
    writeContent(urlConn, createJSONString("TestInstance", null, -1));
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void announceInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/announce-instance", HttpMethod.POST);
    writeContent(urlConn, createJSONString("TestInstance", "127.0.0.1", 1111));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void discoverInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-instance/TestInstance", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 1111), getContent(urlConn));
    urlConn.disconnect();
  }

  private void badDiscoverInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-instance/BadTestInstance", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void badDiscoverInitializedInstance1() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-initialized-instance/TestInstance", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void announceInitializedInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/announce-initialized-instance", HttpMethod.POST);
    writeContent(urlConn, createJSONString("TestInstance", null, -1));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void discoverInitializedInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-initialized-instance/TestInstance", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 1111), getContent(urlConn));
    urlConn.disconnect();
  }

  private void badDiscoverInitializedInstance() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-initialized-instance/BadTestInstance", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void dataSource() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-source/source1", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 8081), getContent(urlConn));
    urlConn = request("/v1/discover-source/source2", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 8082), getContent(urlConn));
    urlConn.disconnect();
  }

  private void badDataSource() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-source/badSource", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void dataSink() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-sink/sink1", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 7081), getContent(urlConn));
    urlConn = request("/v1/discover-sink/sink2", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals(createJSONString(null, "127.0.0.1", 7082), getContent(urlConn));
    urlConn.disconnect();
  }

  private void badDataSink() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-sink/badSink", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void earlyDiscoverStartProcessing() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-start-processing/TestInstance", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void announceStreamSubscription() throws IOException {
    HttpURLConnection urlConn = request("/v1/announce-stream-subscription", HttpMethod.POST);
    writeContent(urlConn, createJSONString("TestInstance", null, -1));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void discoverStartProcessing() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-start-processing/TestInstance", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals("{}", getContent(urlConn));
    urlConn.disconnect();
  }

  private void badDiscoverStartProcessing() throws IOException {
    HttpURLConnection urlConn = request("/v1/discover-start-processing/test", HttpMethod.GET);
    Assert.assertEquals(400, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void announceMetrics() throws IOException {
    HttpURLConnection urlConn = request("/v1/announce-fta-instance", HttpMethod.POST);
    writeContent(urlConn, createJSONString("TestInstance", null, -1, "testProcessID", null, "testProcessName"));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private void logMetrics() throws IOException {
    HttpURLConnection urlConn = request("/v1/log-metrics", HttpMethod.POST);
    JsonObject metrics = new JsonObject();
    metrics.addProperty("TestCounter", "99");
    writeContent(urlConn, createJSONString("TestInstance", null, -1, "testProcessID", metrics));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }
}
