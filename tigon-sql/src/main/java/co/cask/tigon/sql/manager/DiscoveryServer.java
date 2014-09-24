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

import co.cask.http.NettyHttpService;
import co.cask.tigon.sql.internal.HealthInspector;
import co.cask.tigon.sql.internal.MetricsRecorder;
import co.cask.tigon.sql.internal.ProcessMonitor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * DiscoveryServer is the internal HTTP server that serves the Stream Engine processes.
 */
public class DiscoveryServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryServer.class);
  private final HubDataStore hubDataStore;
  private final HealthInspector inspector;
  private final MetricsRecorder metricsRecorder;
  private final ProcessMonitor processMonitor;

  private NettyHttpService service;
  private InetSocketAddress serviceAddress;

  @Override
  public void startUp() {
    HubHttpHandler handler = new HubHttpHandler(hubDataStore, inspector, metricsRecorder, processMonitor);
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableList.of(handler));
    service = builder.build();
    service.startAndWait();
    serviceAddress = service.getBindAddress();
    LOG.info("Started DiscoveryServer HTTP Service at {}", serviceAddress);
    handler.setHubAddress(serviceAddress);
  }

  @Override
  public void shutDown() {
    service.stopAndWait();
  }

  public DiscoveryServer(HubDataStore ds, HealthInspector inspector, MetricsRecorder metricsRecorder
    , ProcessMonitor monitor) {
    hubDataStore = ds;
    this.inspector = inspector;
    this.metricsRecorder = metricsRecorder;
    this.processMonitor = monitor;
  }

  public InetSocketAddress getHubAddress() {
    return serviceAddress;
  }
}
