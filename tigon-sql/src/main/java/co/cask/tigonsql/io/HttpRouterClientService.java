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

package co.cask.tigonsql.io;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

/**
 * HTTP Data Ingestion Router to TCP Server Routing via TCPClientService.
 */
//TODO: Remove this class. Wrap the DataSourceServer which exposes a write method and both the TCP Ingestion Server
//and the DataIngestion Router NettyHTTP Service can write to it directly.
public class HttpRouterClientService extends AbstractIdleService {
  private final Map<String, InetSocketAddress> serverMap;
  private final ConcurrentMap<String, Channel> channelList = Maps.newConcurrentMap();
  private final ClientBootstrap clientBootstrap;

  public HttpRouterClientService(Map<String, InetSocketAddress> ingestionServerMap) {
    this.serverMap = ingestionServerMap;
    ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                               Executors.newCachedThreadPool());
    this.clientBootstrap = new ClientBootstrap(factory);
  }

  @Override
  protected void startUp() throws Exception {
    setupClientPipeline();
    for (final Map.Entry<String, InetSocketAddress> entry : serverMap.entrySet()) {
      clientBootstrap.connect(entry.getValue()).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          channelList.put(entry.getKey(), future.getChannel());
        }
      });
    }
  }

  @Override
  protected void shutDown() throws Exception {
    for (Map.Entry<String, Channel> clientChannel : channelList.entrySet()) {
      clientChannel.getValue().close();
    }
  }

  public boolean sendData(String channelName, ChannelBuffer data) {
    Channel client = channelList.get(channelName);
    if (client != null && client.isConnected()) {
      client.write(data);
      return true;
    }
    return false;
  }

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
}
