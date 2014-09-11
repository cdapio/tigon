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

package co.cask.tigon.sql.internal;

import co.cask.tigon.sql.flowlet.GDATRecordQueue;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import co.cask.tigon.sql.ioserver.StreamEngineIO;
import co.cask.tigon.sql.manager.DiscoveryServer;
import co.cask.tigon.sql.manager.HubDataSink;
import co.cask.tigon.sql.manager.HubDataSource;
import co.cask.tigon.sql.manager.HubDataStore;
import co.cask.tigon.sql.manager.ProcessInitiator;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Services;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Starts/Shutdowns Netty TCP Servers for I/O with StreamEngine Processes, DiscoveryServer HTTP Service and
 * Stream Engine processes.
 */
public final class InputFlowletService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(InputFlowletService.class);
  private final Location dir;
  private final StreamEngineIO ioService;
  private HubDataStore hubDataStore;
  private DiscoveryServer discoveryServer;
  private final HealthInspector healthInspector;
  private final MetricsRecorder metricsRecorder;
  private ProcessInitiator processInitiator;

  //TODO Remove GDATRecordQueue parameter from this constructor. Use Guice to inject it directly to OutputServerSocket
  public InputFlowletService(Location dir, InputFlowletSpecification spec, HealthInspector healthInspector,
                             MetricsRecorder metricsRecorder, GDATRecordQueue recordQueue) {
    this.dir = dir;
    this.ioService = new StreamEngineIO(spec, recordQueue);
    this.healthInspector = healthInspector;
    this.metricsRecorder = metricsRecorder;
  }

  @Override
  protected void startUp() {
    LOG.info("Stream Engine Binaries Location : {}", dir.toURI().getPath());
    LOG.info("Starting IO Socket Servers");
    ioService.startAndWait();
    LOG.info("TCP Ingestion Servers : {}", ioService.getInputServerMap());

    Map<String, InetSocketAddress> inputServer = ioService.getDataSourceServerMap();
    List<HubDataSource> inputServerList = Lists.newArrayList();
    for (Map.Entry<String, InetSocketAddress> entry : inputServer.entrySet()) {
      inputServerList.add(new HubDataSource(entry.getKey(), entry.getValue()));
    }

    Map<String, InetSocketAddress> outputServer = ioService.getDataSinkServerMap();
    List<HubDataSink> outputServerList = Lists.newArrayList();
    for (Map.Entry<String, InetSocketAddress> entry : outputServer.entrySet()) {
      outputServerList.add(new HubDataSink(entry.getKey(), entry.getKey(), entry.getValue()));
    }

    hubDataStore = new HubDataStore.Builder()
      .setHubAddress(ioService.getDataRouterEndpoint())
      .setDataSource(inputServerList)
      .setDataSink(outputServerList)
      .setInstanceName(Constants.STREAMENGINE_INSTANCE)
      .setBinaryLocation(dir)
      .build();

    startService();
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping IO Socket Servers");
    Services.chainStop(ioService, processInitiator, discoveryServer);
  }

  public void startService() {
    //Initializing discovery server
    discoveryServer = new DiscoveryServer(hubDataStore, healthInspector, metricsRecorder);
    discoveryServer.startAndWait();

    //Initiating SQL Compiler processes
    processInitiator = new ProcessInitiator(new HubDataStore.Builder(hubDataStore)
                                              .setHubAddress(discoveryServer.getHubAddress())
                                              .build());
    processInitiator.startAndWait();
  }

  public void restartService() {
    Services.chainStop(processInitiator, discoveryServer);
    startService();
  }
}
