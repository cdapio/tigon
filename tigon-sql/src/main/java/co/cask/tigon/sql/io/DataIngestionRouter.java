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

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Netty Http Service Endpoint for the Users to ingest data.
 */
public class DataIngestionRouter extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DataIngestionRouter.class);
  private final HttpRouterClientService clientService;
  private int httpPort;
  private NettyHttpService httpService;

  public DataIngestionRouter(Map<String, InetSocketAddress> ingestionServerMap) {
    this(ingestionServerMap, 0);
  }

  public DataIngestionRouter(Map<String, InetSocketAddress> ingestionServerMap,
                             int httpPort) {
    this.clientService = new HttpRouterClientService(ingestionServerMap);
    this.httpPort = httpPort;
  }

  @Override
  protected void startUp() throws Exception {
    httpService = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(new ForwardingHandler(clientService)))
      .setPort(httpPort)
      .build();
    httpService.addListener(new ServiceListenerAdapter() {
      private Cancellable cancellable;

      @Override
      public void running() {
        final InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.info("Data Ingestion Router HTTP Service started at {}", socketAddress);
      }

      @Override
      public void terminated(State from) {
        LOG.info("Data Ingestion Router HTTP Service stopped");
        if (cancellable != null) {
          cancellable.cancel();
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("Data Ingestion Router HTTP Service stopped with failure", failure);
        if (cancellable != null) {
          cancellable.cancel();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    clientService.startAndWait();
    httpService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
    clientService.stopAndWait();
  }

  public InetSocketAddress getAddress() {
    return httpService.getBindAddress();
  }

  /**
   * HTTP Endpoint handler method.
   */
  @Path("/v1/tigon")
  public static class ForwardingHandler extends AbstractHttpHandler {
    private final HttpRouterClientService clientService;

    public ForwardingHandler(HttpRouterClientService clientService) {
      this.clientService = clientService;
    }

    @Path("{streamname}")
    @POST
    public void ingestData(HttpRequest request, HttpResponder responder, @PathParam("streamname") String streamName) {
      //Forward the data to the correct TCP endpoint based on the stream name.
      //TODO: Check if the stream name is valid and return NOT_FOUND if stream name is not present
      if (clientService.sendData(streamName, request.getContent())) {
        responder.sendStatus(HttpResponseStatus.OK);
        return;
      }
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
