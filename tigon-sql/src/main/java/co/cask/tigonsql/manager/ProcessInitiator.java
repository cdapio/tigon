/**
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.tigonsql.manager;

import co.cask.tigonsql.util.MetaInformationParser;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * ProcessInitiator is responsible for initiating all processes required by Streaming Engine.
 */

public class ProcessInitiator extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessInitiator.class);
  private final HubDataStore hubDataStore;
  private final List<ExternalProgramExecutor> rtsExecutor;
  private final List<ExternalProgramExecutor> hftaExecutor;
  private final List<ExternalProgramExecutor> gsExitExecutor;
  private final Location binLocation;

  /**
   * Constructs ProcessInitiator object for the specified HubDataStore.
   * @param ds associated HubDataStore
   */
  public ProcessInitiator(HubDataStore ds) {
    hubDataStore = ds;
    rtsExecutor = Lists.newArrayList();
    hftaExecutor = Lists.newArrayList();
    gsExitExecutor = Lists.newArrayList();
    binLocation = hubDataStore.getBinaryLocation();
  }

  /**
   * Sequentially invokes the required Streaming Engine processes.
   */
  @Override
  protected void startUp() {
    try {
      startRTS();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot initiate RTS. Missing or bad arguments");
    }
    try {
      startHFTA();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot initiate HFTA processes. Missing or bad arguments");
    }
    try {
      startGSEXIT();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot initiate GSEXIT processes. Missing or bad arguments");
    }
  }

  /**
   * Stop Stream Engine processes.
   */
  @Override
  protected void shutDown() throws Exception {
    stopRTS();
    stopHFTA();
    stopGSEXIT();
  }

  private String getHostPort(InetSocketAddress address) {
    return address.getAddress().getHostAddress() + ":" + address.getPort();
  }

  /**
   * Starts RTS process.
   * @throws IOException
   */
  public void startRTS() throws IOException {
    List<HubDataSource> dataSources = hubDataStore.getHubDataSources();
    List<String> com = Lists.newArrayList();
    com.add(getHostPort(hubDataStore.getHubAddress()));
    com.add(hubDataStore.getInstanceName());
    for (HubDataSource source : dataSources) {
      com.add(source.getName());
    }
    ExternalProgramExecutor executorService = new ExternalProgramExecutor(
      "RTS", binLocation.append("rts"), com.toArray(new String[com.size()]));
    rtsExecutor.add(executorService);
    LOG.info("Starting RTS : {}", executorService);
    executorService.startAndWait();
  }

  /**
   * Starts HFTA processes.
   * @throws IOException
   */
  public void startHFTA() throws IOException {
    int hftaCount = MetaInformationParser.getHFTACount(new File(this.hubDataStore.getBinaryLocation().toURI()));
    for (int i = 0; i < hftaCount; i++) {
      List<String> com = Lists.newArrayList();
      com.add(getHostPort(hubDataStore.getHubAddress()));
      com.add(hubDataStore.getInstanceName());
      ExternalProgramExecutor executorService = new ExternalProgramExecutor(
        "HFTA-" + i, binLocation.append("hfta_" + i), com.toArray(new String[com.size()]));
      hftaExecutor.add(executorService);
      LOG.info("Starting HFTA : {}", executorService);
      executorService.startAndWait();
    }
  }

  /**
   * Starts GSEXIT processes.
   * @throws IOException
   */
  public void startGSEXIT() throws IOException {
    List<HubDataSink> dataSinks = hubDataStore.getHubDataSinks();
    for (HubDataSink hubDataSink : dataSinks) {
      List<String> com = Lists.newArrayList();
      com.add(getHostPort(hubDataStore.getHubAddress()));
      com.add(hubDataStore.getInstanceName());
      com.add(hubDataSink.getFTAName());
      com.add(hubDataSink.getName());
      ExternalProgramExecutor executorService = new ExternalProgramExecutor(
        "GSEXIT", binLocation.append("GSEXIT"), com.toArray(new String[com.size()]));
      gsExitExecutor.add(executorService);
      LOG.info("Starting GSEXIT : {}", executorService);
      executorService.startAndWait();
    }
  }

  public void stopRTS() {
    for (ExternalProgramExecutor p : rtsExecutor) {
      p.stopAndWait();
    }
  }

  public void stopHFTA() {
    for (ExternalProgramExecutor p : hftaExecutor) {
      p.stopAndWait();
    }
  }

  public void stopGSEXIT() {
    for (ExternalProgramExecutor p : gsExitExecutor) {
      p.stopAndWait();
    }
  }
}
