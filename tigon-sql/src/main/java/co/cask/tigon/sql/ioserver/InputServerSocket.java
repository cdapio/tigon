/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.internal.StreamInputHeader;
import co.cask.tigon.sql.util.GDATFormatUtil;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server Socket for Input Stream to Stream Engine.
 * Starts two Netty Servers - ingestionServer, dataSourceServer.
 *
 * IngestionServer - TCP endpoint for users to connect (or DataIngestionRouter clients to connect) to write data
 * for the given input stream (the stream has a fixed {@link co.cask.tigon.sql.flowlet.StreamSchema}).
 *
 * DataSourceServer - TCP endpoint for StreamEngine Process (RTS) process to connect and receive data.
 *
 * RelayChannel - IngestionServer passes on the data it receives (potential after transforming it to GDAT format) to
 * the StreamEngine TCP client via the Relay Channel.
 */
public class InputServerSocket extends StreamSocketServer {
  private static final Logger LOG = LoggerFactory.getLogger(InputServerSocket.class);
  private final String streamName;
  private final StreamSchema schema;

  private final ServerBootstrap ingestionServer;
  private final ServerBootstrap dataSourceServer;

  private final AtomicReference<Channel> channelAtomicReference;

  public InputServerSocket(ChannelFactory factory, String name, StreamSchema inputSchema) {
    this.streamName = name;
    this.schema = inputSchema;
    this.channelAtomicReference = new AtomicReference<Channel>();
    this.channelAtomicReference.set(null);
    ingestionServer = new ServerBootstrap(factory);
    dataSourceServer = new ServerBootstrap(factory);
  }

  @Override
  public final void startUp() {
    LOG.info("Input Stream {} : Starting Server", streamName);
    setIngestionPipeline();
    setDataSourcePipeline();
    Channel ch = ingestionServer.bind(new InetSocketAddress(0));
    serverAddressMap.put(Constants.StreamIO.TCP_DATA_INGESTION, (InetSocketAddress) ch.getLocalAddress());
    ch = dataSourceServer.bind(new InetSocketAddress(0));
    serverAddressMap.put(Constants.StreamIO.DATASOURCE, (InetSocketAddress) ch.getLocalAddress());
  }

  @Override
  public final void shutDown() throws IOException, InterruptedException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Channel relayChannel = channelAtomicReference.get();
    if (relayChannel != null && relayChannel.isConnected()) {
      GDATFormatUtil.encodeEOF(schema, outputStream);
      //TODO: Use OutStream in GDATEncoder to avoid making toByteArray call since it makes a copy.
      ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(outputStream.toByteArray());
      relayChannel.write(buffer).await(1, TimeUnit.SECONDS);
    }
    LOG.info(String.format("Input Stream %s : Stopping Server", streamName));
    ingestionServer.shutdown();
    dataSourceServer.shutdown();
  }

  private void setIngestionPipeline() {
    setServerOptions(ingestionServer);
    ingestionServer.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        LinkedHashMap<String, ChannelHandler> transformationHandlers = addTransformHandler();
        if (transformationHandlers != null) {
          for (Map.Entry<String, ChannelHandler> handler : transformationHandlers.entrySet()) {
            pipeline.addLast(handler.getKey(), handler.getValue());
          }
        }
        pipeline.addLast("relayData", new RelayDataHandler(streamName));
        return pipeline;
      }
    });
  }

  private void setDataSourcePipeline() {
    setServerOptions(dataSourceServer);
    dataSourceServer.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("writeData", new WriteDataHandler(streamName, schema));
        return pipeline;
      }
    });
  }

  /**
   * Passes on the data it receives from TransformationHandlers (must be in GDAT Format) to the Relay Channel.
   */
  private class RelayDataHandler extends SimpleChannelHandler {
    private final Logger log = LoggerFactory.getLogger(RelayDataHandler.class);
    private final String streamName;

    public RelayDataHandler(String streamName) {
      this.streamName = streamName;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      final Channel ch = e.getChannel();
      ChannelBuffer buf = (ChannelBuffer) e.getMessage();
      Channel relayChannel = channelAtomicReference.get();
      if (relayChannel != null && relayChannel.isConnected()) {
        //TODO: Handle channel saturation
        //Writing it to the downstream server pipeline
        relayChannel.write(buf);
      } else {
        //TODO: Do we want to print an error msg for every dropped packet?
        log.error("Input Stream {} : DataSource Server not connected to a client! Dropping input data!", streamName);
      }
    }
  }

  /**
   * Saves the StreamEngine client connection channel to channelAtomicReference when it connects.
   */
  private class WriteDataHandler extends SimpleChannelHandler {
    private final Logger log = LoggerFactory.getLogger(WriteDataHandler.class);
    private final String header;
    private final String name;

    public WriteDataHandler(String name, StreamSchema schema) {
      this.name = name;
      this.header = new StreamInputHeader(name, schema).getStreamHeader();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      //TODO: Close any other connections if a connection already exists!
      final Channel ch = e.getChannel();
      ChannelBuffer headerBuffer = ChannelBuffers.buffer(header.length());
      headerBuffer.writeBytes(header.getBytes());
      ChannelFuture f = ch.write(headerBuffer);
      f.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      channelAtomicReference.set(ch);
      log.info("Input Stream {} : Channel Connected. Sent Header : {}", name, header);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      //Stream Engine RTS process Disconnected!
      log.error("Input Stream {} : Channel Disconnected - Stream Engine RTS process!", name);
      channelAtomicReference.set(null);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      //TODO: How to handle exceptions?
      log.error("Input Stream {} : {}", name, e.getCause());
      Channel ch = e.getChannel();
      ch.close();
      channelAtomicReference.set(null);
    }
  }

  /**
   * Subclasses can override this method to add handlers for transforming the incoming data format to GDAT byte array.
   */
  protected LinkedHashMap<String, ChannelHandler> addTransformHandler() {
    return null;
  }
}
