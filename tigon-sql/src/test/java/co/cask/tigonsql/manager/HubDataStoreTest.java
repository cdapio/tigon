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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * HubDataStoreTest
 */

public class HubDataStoreTest {
  private static HubDataStore hubDataStore;

  @BeforeClass
  public static void setup() throws Exception {
    List<HubDataSource> sourceList = Lists.newArrayList();
    sourceList.add(new HubDataSource("source1", new InetSocketAddress("127.0.0.1", 8081)));
    sourceList.add(new HubDataSource("source2", new InetSocketAddress("127.0.0.1", 8082)));
    List<HubDataSink> sinkList = Lists.newArrayList();
    sinkList.add(new HubDataSink("sink1", "query1", new InetSocketAddress("127.0.0.1", 7081)));
    sinkList.add(new HubDataSink("sink2", "query2", new InetSocketAddress("127.0.0.1", 7082)));
    hubDataStore = new HubDataStore.Builder()
      .setInstanceName("test")
      .setDataSource(sourceList)
      .setDataSink(sinkList)
      .setHFTACount(5)
      .setHubAddress(new InetSocketAddress("127.0.0.1", 8080))
      .setClearingHouseAddress(new InetSocketAddress("127.0.0.1", 9090))
      .build();
  }

  @Test
  public void testDataSources() {
    List<HubDataSource> dataSources = hubDataStore.getHubDataSources();
    Assert.assertEquals("source1", dataSources.get(0).getName());
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 8081), dataSources.get(0).getAddress());
    Assert.assertEquals("source2", dataSources.get(1).getName());
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 8082), dataSources.get(1).getAddress());
  }

  @Test
  public void testDataSinks() {
    List<HubDataSink> dataSinks = hubDataStore.getHubDataSinks();
    Assert.assertEquals("sink1", dataSinks.get(0).getName());
    Assert.assertEquals("query1", dataSinks.get(0).getFTAName());
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 7081), dataSinks.get(0).getAddress());
    Assert.assertEquals("sink2", dataSinks.get(1).getName());
    Assert.assertEquals("query2", dataSinks.get(1).getFTAName());
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 7082), dataSinks.get(1).getAddress());
  }

  @Test
  public void testHFTACount() {
    Assert.assertEquals(5, hubDataStore.getHFTACount());
  }

  @Test
  public void testHubAddress() {
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 8080), hubDataStore.getHubAddress());
  }

  @Test
  public void testClearingHouse() {
    Assert.assertEquals(new InetSocketAddress("127.0.0.1", 9090), hubDataStore.getClearingHouseAddress());
  }

  @Test
  public void testInstanceName() {
    Assert.assertEquals("test", hubDataStore.getInstanceName());
  }
}
