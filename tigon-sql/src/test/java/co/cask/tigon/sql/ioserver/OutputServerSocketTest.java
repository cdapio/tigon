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

package co.cask.tigon.sql.ioserver;

import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATRecordQueue;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.internal.StreamInputHeader;
import co.cask.tigon.sql.io.GDATEncoder;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang.ArrayUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test OutputServerSocket by sending GDAT Header and Data bytes in several chunks with delay in between.
 */
public class OutputServerSocketTest {
  private static final Logger LOG = LoggerFactory.getLogger(OutputServerSocketTest.class);
  private static final String schemaName = "purchaseStream";
  private static final StreamSchema testSchema = new StreamSchema.Builder()
    .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
    .addField("itemid", GDATFieldType.STRING)
    .build();
  private static ChannelFactory clientFactory;
  private static ChannelFactory serverFacotry;
  private static ClientBootstrap clientBootstrap;

  private void setupClientPipeline() {
    clientBootstrap.setOption("tcpNoDelay", true);
    clientBootstrap.setOption("keepAlive", true);
    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("passHandler", new PassHandler());
        return pipeline;
      }
    });
  }

  private static class PassHandler extends SimpleChannelHandler {

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      super.writeRequested(ctx, e);
    }
  }

  @BeforeClass
  public static void init() {
    clientFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    serverFacotry = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    clientBootstrap = new ClientBootstrap(clientFactory);
  }

  @Test
  public void testStreamHeaderParser() throws InterruptedException, IOException {
    String outputName = "output";
    GDATEncoder encoder = new GDATEncoder();
    encoder.writeLong(3L);
    encoder.writeString("Hello");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    encoder.writeTo(out);
    byte[] dataBytes = out.toByteArray();
    OutputServerSocket outputServerSocket = new OutputServerSocket(serverFacotry, outputName, "SELECT foo FROM bar",
                                                                   new GDATRecordQueue());
    outputServerSocket.startAndWait();
    InetSocketAddress serverAddress = outputServerSocket.getSocketAddressMap().get(Constants.StreamIO.DATASINK);
    LOG.info("Server is running at {}", serverAddress);
    setupClientPipeline();
    ChannelFuture future = clientBootstrap.connect(serverAddress);
    future.await(3, TimeUnit.SECONDS);
    Channel channel = future.getChannel();
    String gdatHeader = new StreamInputHeader(outputName, testSchema).getStreamHeader();
    List<Byte> gdatByteList = new ArrayList<Byte>(Bytes.asList(gdatHeader.getBytes(Charsets.UTF_8)));
    gdatByteList.addAll(new ArrayList<Byte>(Bytes.asList(dataBytes)));

    int partitionSize = 10;
    List<List<Byte>> byteChunks = Lists.newArrayList();
    for (int i = 0; i < gdatByteList.size(); i += partitionSize) {
      byteChunks.add(gdatByteList.subList(i, i + Math.min(partitionSize, gdatByteList.size() - i)));
    }

    ChannelBuffer buffer;
    for (List<Byte> chunk : byteChunks) {
      buffer = ChannelBuffers.wrappedBuffer(ArrayUtils.toPrimitive(chunk.toArray(new Byte[chunk.size()])));
      channel.write(buffer).await();
      TimeUnit.MILLISECONDS.sleep(10);
    }
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(1, outputServerSocket.getDataRecordsReceived());
  }
}
