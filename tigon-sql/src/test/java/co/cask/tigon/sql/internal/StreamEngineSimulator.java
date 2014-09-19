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

package co.cask.tigon.sql.internal;

import com.google.common.util.concurrent.AbstractIdleService;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Simulates StreamEngine - Identity transform of single input stream to single output stream.
 */
public class StreamEngineSimulator extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(StreamEngineSimulator.class);
  private static Channel clientChannel;
  private static Channel dataSinkChannel;
  private final InetSocketAddress dataSourceServerSocket;
  private final InetSocketAddress ingestionServerSocket;
  private final InetSocketAddress outputServerSocket;
  private final ClientBootstrap userClient;
  private final ClientBootstrap dataSourceClient;
  private final ClientBootstrap dataSinkClient;

  public StreamEngineSimulator(InetSocketAddress dataSourceServerSocket, InetSocketAddress outputServerSocket,
                               InetSocketAddress ingestionServerSocket) {
    this.dataSourceServerSocket = dataSourceServerSocket;
    this.outputServerSocket = outputServerSocket;
    this.ingestionServerSocket = ingestionServerSocket;
    ChannelFactory clientFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                     Executors.newCachedThreadPool());
    this.userClient = new ClientBootstrap(clientFactory);
    this.dataSourceClient = new ClientBootstrap(clientFactory);
    this.dataSinkClient = new ClientBootstrap(clientFactory);
  }

  @Override
  protected void startUp() throws Exception {
    setupUserClientPipeline();
    setupDataSourceClientPipeline();
    setupDataSinkClientPipeline();

    if (outputServerSocket != null) {
      dataSinkChannel = dataSinkClient.connect(outputServerSocket).awaitUninterruptibly().getChannel();
      Assert.assertTrue(dataSinkChannel.isConnected());
    }

    Channel dsChannel = dataSourceClient.connect(dataSourceServerSocket).awaitUninterruptibly().getChannel();
    Assert.assertTrue(dsChannel.isConnected());

    if (ingestionServerSocket != null) {
      clientChannel = userClient.connect(ingestionServerSocket).awaitUninterruptibly().getChannel();
      Assert.assertTrue(clientChannel.isConnected());
    }
  }

  @Override
  protected void shutDown() throws Exception {
    userClient.shutdown();
    dataSourceClient.shutdown();
    dataSinkClient.shutdown();
  }

  private void setClientOptions(ClientBootstrap clientBootstrap) {
    clientBootstrap.setOption("tcpNoDelay", true);
    clientBootstrap.setOption("keepAlive", true);
  }

  private void setupUserClientPipeline() {
    setClientOptions(userClient);
    userClient.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("passHandler", new PassHandler());
        return pipeline;
      }
    });
  }

  private void setupDataSourceClientPipeline() {
    setClientOptions(dataSourceClient);
    dataSourceClient.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("relayChannel", new RelaySourceToSinkHandler());
        return pipeline;
      }
    });
  }

  private static class RelaySourceToSinkHandler extends SimpleChannelHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RelaySourceToSinkHandler.class);
    private int relayedBytes = 0;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      if (dataSinkChannel != null && dataSinkChannel.isConnected()) {
        relayedBytes += ((ChannelBuffer) e.getMessage()).readableBytes();
        LOG.info("Relayed Bytes " + relayedBytes);
        dataSinkChannel.write(e.getMessage());
      } else {
        LOG.info("Relaying from Source to Sink Client didn't happen! Dropping Data");
      }
    }
  }

  private void setupDataSinkClientPipeline() {
    setClientOptions(dataSinkClient);
    dataSinkClient.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("passHandler", new PassHandler());
        return pipeline;
      }
    });
  }

  private static class PassHandler extends SimpleChannelHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PassHandler.class);
    private int passedBytes = 0;

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      passedBytes += ((ChannelBuffer) e.getMessage()).readableBytes();
      LOG.info("Passed Bytes " + passedBytes);
      super.writeRequested(ctx, e);
    }
  }

  //Send data to the ingestion server.
  public void sendData(byte[] dataRecord) throws Exception {
    if (clientChannel != null && clientChannel.isConnected()) {
      LOG.info("Writing Data Record");
      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
      buffer.writeBytes(dataRecord);
      clientChannel.write(buffer);
    } else {
      LOG.warn("Dropping Data Record since there is no server to connect to");
    }
  }
}
