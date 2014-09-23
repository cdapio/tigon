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

package co.cask.tigon.sql.io;

import co.cask.tigon.sql.conf.Constants;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test Data Ingestion Router and Client Service.
 */
public class DataRouterTest {
  private static final Map<String, InetSocketAddress> serverMap = Maps.newHashMap();
  private static final Map<String, Integer> testMap = Maps.newHashMap();
  private static ServerBootstrap serverBootstrap;

  private static class DataCounter extends SimpleChannelHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      String key = (String) e.getMessage();
      if (!testMap.containsKey(key)) {
        testMap.put(key, 1);
      } else {
        testMap.put(key, testMap.get(key) + 1);
      }
    }
  }

  @BeforeClass
  public static void init() throws IOException {
    serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                            Executors.newCachedThreadPool()));
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("delimiter", new LineBasedFrameDecoder(Integer.MAX_VALUE));
        pipeline.addLast("stringDecoder", new StringDecoder());
        pipeline.addLast("counter", new DataCounter());
        return pipeline;
      }
    });

    serverMap.put("stream1", (InetSocketAddress) serverBootstrap.bind(new InetSocketAddress(0)).getLocalAddress());
    serverMap.put("stream2", (InetSocketAddress) serverBootstrap.bind(new InetSocketAddress(0)).getLocalAddress());
    serverMap.put("stream3", (InetSocketAddress) serverBootstrap.bind(new InetSocketAddress(0)).getLocalAddress());
    serverMap.put("stream4", (InetSocketAddress) serverBootstrap.bind(new InetSocketAddress(0)).getLocalAddress());
  }

  @Test
  public void testDataRouter() throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    DataIngestionRouter router = new DataIngestionRouter(discoveryService, serverMap);
    try {
      router.startAndWait();
      TimeUnit.SECONDS.sleep(1);
      Discoverable discoverable = Iterables.getFirst(discoveryService.discover(Constants.StreamIO.HTTP_DATA_INGESTION),
                                                     null);
      InetSocketAddress routerAddress = discoverable.getSocketAddress();
      HttpHost routerHost = new HttpHost(routerAddress.getHostName(), routerAddress.getPort());
      doPost(routerHost, "stream1");
      doPost(routerHost, "stream2");
      doPost(routerHost, "stream1");
      doPost(routerHost, "stream3");
      doPost(routerHost, "stream2");
      doPost(routerHost, "stream2");
      Assert.assertEquals(2, testMap.get("stream1").intValue());
      Assert.assertEquals(3, testMap.get("stream2").intValue());
      Assert.assertEquals(1, testMap.get("stream3").intValue());
      Assert.assertTrue(!testMap.containsKey("stream4"));
    } finally {
      router.stopAndWait();
    }
  }

  private void doPost(HttpHost host, String streamName) throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    HttpPost httpPost = new HttpPost("/v1/tigon/" + streamName);
    httpPost.setEntity(new StringEntity(streamName + "\n"));
    httpClient.execute(host, httpPost);
  }

  @AfterClass
  public static void finish() {
    serverBootstrap.shutdown();
  }
}
