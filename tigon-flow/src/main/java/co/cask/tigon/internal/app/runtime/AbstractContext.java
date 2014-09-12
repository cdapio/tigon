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

package co.cask.tigon.internal.app.runtime;

import co.cask.tigon.api.RuntimeContext;
import co.cask.tigon.api.metrics.Metrics;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.discovery.EndpointStrategy;
import co.cask.tigon.discovery.RandomEndpointStrategy;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.MetricsCollector;
import co.cask.tigon.metrics.MetricsScope;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.NoSuchElementException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext implements RuntimeContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;

  private final MetricsCollector programMetrics;
  private final DiscoveryServiceClient discoveryServiceClient;

  public AbstractContext(Program program, RunId runId,
                         String metricsContext,
                         MetricsCollectionService metricsCollectionService,
                         CConfiguration conf,
                         DiscoveryServiceClient discoveryServiceClient) {
    this.program = program;
    this.runId = runId;
    this.discoveryServiceClient = discoveryServiceClient;

    if (metricsCollectionService != null) {
      // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
      this.programMetrics = metricsCollectionService.getCollector(MetricsScope.REACTOR, metricsContext, "0");
    } else {
      this.programMetrics = null;
    }
  }

  public abstract Metrics getMetrics();

  @Override
  public String toString() {
    return String.format("accountId=%s, applicationId=%s, program=%s, runid=%s",
                         getAccountId(), getApplicationId(), getProgramName(), runId);
  }

  public MetricsCollector getProgramMetrics() {
    return programMetrics;
  }

  public String getAccountId() {
    return program.getAccountId();
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  @Override
  public URL getServiceURL(final String applicationId, final String serviceId) {
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(String.format("service.%s.%s.%s",
                                                                                        getAccountId(),
                                                                                        applicationId,
                                                                                        serviceId));
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(serviceDiscovered);
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable != null) {
      return createURL(discoverable, applicationId, serviceId);
    }

    final SynchronousQueue<URL> discoverableQueue = new SynchronousQueue<URL>();
    Cancellable discoveryCancel = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        try {
          URL url = createURL(serviceDiscovered.iterator().next(), applicationId, serviceId);
          discoverableQueue.offer(url);
        } catch (NoSuchElementException e) {
          LOG.debug("serviceDiscovered is empty");
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      URL url = discoverableQueue.poll(1, TimeUnit.SECONDS);
      if (url == null) {
        LOG.debug("Discoverable endpoint not found for appID: {}, serviceID: {}.", applicationId, serviceId);
      }
      return url;
    } catch (InterruptedException e) {
      LOG.error("Got exception: ", e);
      return null;
    } finally {
      discoveryCancel.cancel();
    }
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(getApplicationId(), serviceId);
  }

  private URL createURL(@Nullable Discoverable discoverable, String applicationId, String serviceId) {
    if (discoverable == null) {
      return null;
    }
    String hostName = discoverable.getSocketAddress().getHostName();
    int port = discoverable.getSocketAddress().getPort();
    String path = String.format("http://%s:%d%s/apps/%s/services/%s/methods/", hostName, port,
                                Constants.Gateway.GATEWAY_VERSION, applicationId, serviceId);
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }

  /**
   * Release all resources held by this context. Subclasses should override this method to release additional resources.
   */
  public void close() {

  }
}
