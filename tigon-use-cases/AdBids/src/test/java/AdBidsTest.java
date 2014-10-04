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

import co.cask.tephra.TransactionContext;
import co.cask.tephra.hbase96.TransactionAwareHTable;
import co.cask.tephra.inmemory.DetachedTxSystemClient;
import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.data.util.hbase.HBase96TableUtil;
import co.cask.tigon.test.FlowManager;
import co.cask.tigon.test.TestBase;
import co.cask.tigon.usecases.adbids.AdBids;
import co.cask.tigon.usecases.adbids.Bid;
import co.cask.tigon.usecases.adbids.Item;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.twill.internal.utils.Networks;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link AdBids} Flow.
 */
public class AdBidsTest extends TestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void testInMemoryAdBids() throws Exception {
    int inputPort = Networks.getRandomPort();
    File outputFile = tmpFolder.newFile();
    Map<String, String>  runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("input.service.port", String.valueOf(inputPort));
    runtimeArguments.put("bids.output.file", outputFile.getAbsolutePath());
    FlowManager flowManager = deployFlow(AdBids.class, runtimeArguments);
    TimeUnit.SECONDS.sleep(5);

    postEvents(inputPort);

    List<String> serializedBids = Files.readLines(outputFile, Charsets.UTF_8);
    List<Bid> bids = Lists.newArrayList();
    for (String serializedBid : serializedBids) {
      Bid bid = GSON.fromJson(serializedBid, Bid.class);
      bids.add(bid);
    }

    Assert.assertEquals(6, bids.size());

    Bid firstBid = bids.get(0);
    Bid secondBid = bids.get(1);
    Bid thirdBid = bids.get(2);

    Assert.assertEquals("test-user-0", firstBid.getId());
    Assert.assertEquals(Item.TRAVEL, firstBid.getItem());
    Assert.assertEquals(12.0, firstBid.getAmount(), 0);

    Assert.assertEquals("test-user-0", firstBid.getId());
    Assert.assertEquals(Item.SPORTS, secondBid.getItem());
    Assert.assertEquals(10.0, secondBid.getAmount(), 0);

    Assert.assertEquals("test-user-0", firstBid.getId());
    Assert.assertEquals(Item.TRAVEL, thirdBid.getItem());
    Assert.assertEquals(6.0, thirdBid.getAmount(), 0);

    Bid fourthBid = bids.get(3);
    Bid fifthBid = bids.get(4);
    Bid sixthBid = bids.get(5);

    Assert.assertEquals("test-user-1", fourthBid.getId());
    Assert.assertEquals(Item.TRAVEL, fourthBid.getItem());
    Assert.assertEquals(12.0, fourthBid.getAmount(), 0);

    Assert.assertEquals("test-user-1", fifthBid.getId());
    Assert.assertEquals(Item.SPORTS, fifthBid.getItem());
    Assert.assertEquals(10.0, fifthBid.getAmount(), 0);

    Assert.assertEquals("test-user-1", sixthBid.getId());
    Assert.assertEquals(Item.TRAVEL, sixthBid.getItem());
    Assert.assertEquals(6.0, sixthBid.getAmount(), 0);

    flowManager.stop();
  }

  @Test
  public void testHbaseAdBids() throws Exception {
    int inputPort = Networks.getRandomPort();
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    HBaseAdmin hBaseAdmin = testUtil.getHBaseAdmin();

    File confFile = tmpFolder.newFile("config.xml");
    FileOutputStream fos = new FileOutputStream(confFile);
    try {
      Configuration configuration = hBaseAdmin.getConfiguration();
      configuration.set("hbase.table.compression.default", "NONE");
      configuration.writeXml(fos);
    } finally {
      fos.close();
    }
    String hbaseConfPath = confFile.getAbsolutePath();

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("input.service.port", String.valueOf(inputPort));
    runtimeArguments.put("hbase.conf.path", hbaseConfPath);

    FlowManager flowManager = deployFlow(AdBids.class, ImmutableList.<Class<?>>of(HBase96TableUtil.class),
                                         runtimeArguments);
    TimeUnit.SECONDS.sleep(20);

    postEvents(inputPort);

    flowManager.stop();
    TimeUnit.SECONDS.sleep(20);

    HTable hTable = new HTable(hBaseAdmin.getConfiguration(), AdBids.BID_TABLE_NAME);
    TransactionAwareHTable txAwareHTable = new TransactionAwareHTable(hTable);
    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(), txAwareHTable);

    for (int i = 0; i < 2; i++) {
      transactionContext.start();
      Result result = txAwareHTable.get(new Get(Bytes.toBytes("test-user-" + i)));
      transactionContext.finish();

      Assert.assertEquals(3, result.size());

      NavigableMap<byte[], byte[]> travelFamilyMap = result.getFamilyMap(Bytes.toBytes(Item.TRAVEL));
      NavigableMap<byte[], byte[]> sportsFamilyMap = result.getFamilyMap(Bytes.toBytes(Item.SPORTS));

      Assert.assertEquals(2, travelFamilyMap.size());
      Assert.assertEquals(1, sportsFamilyMap.size());

      assertHbaseBids(travelFamilyMap.pollFirstEntry(), 12.0);
      assertHbaseBids(sportsFamilyMap.pollFirstEntry(), 10.0);
      assertHbaseBids(travelFamilyMap.pollFirstEntry(), 6.0);
    }

    hBaseAdmin.disableTable(AdBids.BID_TABLE_NAME);
    hBaseAdmin.deleteTable(AdBids.BID_TABLE_NAME);
    hBaseAdmin.close();
    testUtil.shutdownMiniCluster();
  }

  private void assertHbaseBids(Map.Entry<byte[], byte[]> entry, double expected) {
    double value = Bytes.toDouble(entry.getValue());
    Assert.assertEquals(expected, value, 0);
  }

  private void postEvents(int port) throws Exception {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        String newCustomerEndpoint = String.format("http://%s:%d/id/test-user-%d", "localhost", port, i);
        HttpURLConnection urlConn = (HttpURLConnection) new URL(newCustomerEndpoint).openConnection();
        urlConn.setRequestMethod("POST");
        urlConn.getResponseCode();
        TimeUnit.SECONDS.sleep(10);
      }
    }
  }
}
