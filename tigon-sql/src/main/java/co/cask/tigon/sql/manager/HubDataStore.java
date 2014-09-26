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

import com.google.common.collect.ImmutableList;
import org.apache.twill.filesystem.Location;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * HubDataStore holds all information associated with Netty IO Servers.
 */

public class HubDataStore {
  private final String instanceName;
  private final boolean initialized;
  private final boolean outputReady;
  private final List<HubDataSource> hubDataSources;
  private final List<HubDataSink> hubDataSinks;
  private final int hftaCount;
  private final InetSocketAddress hubAddress;
  private final InetSocketAddress clearingHouseAddress;
  private final Location binaryLocation;

  private HubDataStore(Builder builder) {
    this.instanceName = builder.instanceName;
    this.initialized = builder.initialized;
    this.outputReady = builder.outputReady;
    this.hftaCount = builder.hftaCount;
    this.hubAddress = builder.hubAddress;
    this.clearingHouseAddress = builder.clearingHouseAddress;
    this.binaryLocation = builder.binaryLocation;

    if (builder.hubDataSources != null) {
      this.hubDataSources = ImmutableList.copyOf(builder.hubDataSources);
    } else {
      this.hubDataSources = Collections.unmodifiableList(new ArrayList<HubDataSource>());
    }
    if (builder.hubDataSinks != null) {
      this.hubDataSinks = ImmutableList.copyOf(builder.hubDataSinks);
    } else {
      this.hubDataSinks = Collections.unmodifiableList(new ArrayList<HubDataSink>());
    }
  }

  /**
   * Returns initialization state for this instance.
   */
  public boolean isInitialized() {
    return initialized;
  }

  /**
   * @return State of the output processes.
   */
  public boolean isOutputReady() {
    return outputReady;
  }

  /**
   * @return immutable list of data source.
   */
  public List<HubDataSource> getHubDataSources() {
    return hubDataSources;
  }

  /**
   * @return immutable list of data sinks.
   */
  public List<HubDataSink> getHubDataSinks() {
    return hubDataSinks;
  }

  /**
   * @return the gsInstance name as a String.
   */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * @return the hub address.
   */
  public InetSocketAddress getHubAddress() {
    return hubAddress;
  }

  /**
   * @return the number of HFTA.
   */
  public int getHFTACount() {
    return hftaCount;
  }

  /**
   *
   * @return Clearing house address (This address is set by the RTS process).
   */
  public InetSocketAddress getClearingHouseAddress() {
    return clearingHouseAddress;
  }

  /**
   *
   * @return location of the binary files as a string.
   */
  public Location getBinaryLocation() {
    return binaryLocation;
  }

  /**
   * Builder to build immutable HubDataStoreObjects.
   */
  public static class Builder {
    private String instanceName;
    private boolean initialized;
    private boolean outputReady;
    private List<HubDataSource> hubDataSources;
    private List<HubDataSink> hubDataSinks;
    private int hftaCount;
    private InetSocketAddress hubAddress;
    private InetSocketAddress clearingHouseAddress;
    private Location binaryLocation;

    public Builder setInstanceName(String name) {
      this.instanceName = name;
      return this;
    }

    public Builder initialize() {
      this.initialized = true;
      return this;
    }

    public Builder setHFTACount(int n) {
      this.hftaCount = n;
      return this;
    }

    public Builder setHubAddress(InetSocketAddress address) {
      this.hubAddress = address;
      return this;
    }

    public Builder setClearingHouseAddress(InetSocketAddress address) {
      this.clearingHouseAddress = address;
      return this;
    }

    public Builder setDataSource(List<HubDataSource> sourceList) {
      this.hubDataSources = sourceList;
      return this;
    }

    public Builder setDataSink(List<HubDataSink> sinkList) {
      this.hubDataSinks = sinkList;
      return this;
    }

    public Builder setBinaryLocation(Location path) {
      this.binaryLocation = path;
      return this;
    }

    public Builder outputReady() {
      this.outputReady = true;
      return this;
    }

    public HubDataStore build() {
      return new HubDataStore(this);
    }

    public Builder(HubDataStore hubDataStore) {
      this.instanceName = hubDataStore.instanceName;
      this.initialized = hubDataStore.initialized;
      this.outputReady = hubDataStore.outputReady;
      this.hubDataSources = hubDataStore.hubDataSources;
      this.hubDataSinks = hubDataStore.hubDataSinks;
      this.hftaCount = hubDataStore.hftaCount;
      this.hubAddress = hubDataStore.hubAddress;
      this.clearingHouseAddress = hubDataStore.clearingHouseAddress;
      this.binaryLocation = hubDataStore.binaryLocation;
    }

    public Builder() {
      this.initialized = false;
      this.hftaCount = 0;
    }

  }
}

