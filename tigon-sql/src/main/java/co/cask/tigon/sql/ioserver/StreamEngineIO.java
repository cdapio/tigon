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

import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.GDATRecordQueue;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import co.cask.tigon.sql.flowlet.InputStreamFormat;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.io.DataIngestionRouter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Handle all I/O Socket Servers for StreamEngine I/O.
 */
public class StreamEngineIO extends AbstractIdleService {
  private final InputFlowletSpecification spec;
  private final List<StreamSocketServer> inputServerSocketServices = Lists.newArrayList();
  private final List<StreamSocketServer> outputServerSocketServies = Lists.newArrayList();
  private final Map<String, Map<String, InetSocketAddress>> inputServerMap = Maps.newHashMap();
  private final Map<String, Map<String, InetSocketAddress>> outputServerMap = Maps.newHashMap();
  private final Map<String, InetSocketAddress> dataIngressServerMap = Maps.newHashMap();
  private final Map<String, InetSocketAddress> dataEgressServerMap = Maps.newHashMap();
  private final Map<String, InetSocketAddress> dataSourceServerMap = Maps.newHashMap();
  private final InMemoryDiscoveryService discoveryService;
  private final GDATRecordQueue recordQueue;
  private DataIngestionRouter router;

  //TODO Remove GDATRecordQueue parameter from this constructor. Use Guice to inject it directly to OutputServerSocket
  public StreamEngineIO(InputFlowletSpecification spec, GDATRecordQueue recordQueue) {
    this.spec = spec;
    this.discoveryService = new InMemoryDiscoveryService();
    this.recordQueue = recordQueue;
  }

  @Override
  public void startUp() throws Exception {
    for (String inputName : spec.getInputSchemas().keySet()) {
      Map.Entry<InputStreamFormat, StreamSchema> streamInfo = spec.getInputSchemas().get(inputName);
      ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                 Executors.newCachedThreadPool());
      StreamSocketServer service;
      switch(streamInfo.getKey()) {
        case GDAT:
          service = new InputServerSocket(factory, inputName, streamInfo.getValue());
          break;

        case JSON:
          service = new JsonInputServerSocket(factory, inputName, streamInfo.getValue());
          break;

        default:
          throw new Exception("Unknown Input Format. Only JSON and GDAT Formats are supported.");
      }

      service.startAndWait();
      inputServerMap.put(inputName, service.getSocketAddressMap());
      dataIngressServerMap.put(inputName, service.getSocketAddressMap().get(Constants.StreamIO.TCP_DATA_INGESTION));
      dataSourceServerMap.put(inputName, service.getSocketAddressMap().get(Constants.StreamIO.DATASOURCE));
      inputServerSocketServices.add(service);
    }

    for (Map.Entry<String, String> output : spec.getQuery().entrySet()) {
      ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                 Executors.newCachedThreadPool());
      StreamSocketServer service = new OutputServerSocket(factory, output.getKey(), output.getValue(), recordQueue);
      service.startAndWait();
      outputServerMap.put(output.getKey(), service.getSocketAddressMap());
      dataEgressServerMap.put(output.getKey(), service.getSocketAddressMap().get(Constants.StreamIO.DATASINK));
      outputServerSocketServies.add(service);
    }

    router = new DataIngestionRouter(discoveryService, dataIngressServerMap);
    router.startAndWait();
  }

  public Map<String, InetSocketAddress> getInputServerMap() {
    return Collections.unmodifiableMap(dataIngressServerMap);
  }

  public Map<String, InetSocketAddress> getDataSourceServerMap() {
    return Collections.unmodifiableMap(dataSourceServerMap);
  }

  public Map<String, InetSocketAddress> getDataSinkServerMap() {
    return Collections.unmodifiableMap(dataEgressServerMap);
  }

  public InetSocketAddress getDataRouterEndpoint() {
    return router.getAddress();
  }

  @Override
  public void shutDown() {
    router.stopAndWait();
    for (StreamSocketServer service : inputServerSocketServices) {
      service.stopAndWait();
    }

    for (StreamSocketServer service : outputServerSocketServies) {
      service.stopAndWait();
    }
  }
}

