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

package co.cask.tigon.flow;

import com.continuuity.tephra.TransactionAware;
import com.continuuity.tephra.TransactionContext;
import com.continuuity.tephra.hbase96.TransactionAwareHTable;
import com.continuuity.tephra.inmemory.DetachedTxSystemClient;
import co.cask.tigon.api.annotation.HashPartition;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Test adding and using {@link TransactionAware}s via {@link FlowletContext}s.
 */
public class TransactionAwareFlowTest extends TestBase {
  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private static HTable hTable;
  private static TransactionAwareHTable transactionAwareHTable;
  private static TransactionContext transactionContext;

  private static final class TestBytes {
    private static final byte[] table = Bytes.toBytes("testTable");
    private static final byte[] family = Bytes.toBytes("testFamily");
    private static final byte[] row = Bytes.toBytes("testRow");
    private static final byte[] processQualifier = Bytes.toBytes("processQualifier");
    private static final byte[] initQualifier = Bytes.toBytes("initQualifier");
    private static final byte[] destroyQualifier = Bytes.toBytes("destroyQualifier");
    private static final byte[] value = Bytes.toBytes("testValue");
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    hBaseAdmin = testUtil.getHBaseAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testUtil.shutdownMiniCluster();
    hBaseAdmin.close();
  }

  @Before
  public void setupBeforeTest() throws IOException {
    hTable = testUtil.createTable(TestBytes.table, TestBytes.family);
    transactionAwareHTable = new TransactionAwareHTable(hTable);
    transactionContext = new TransactionContext(new DetachedTxSystemClient(), transactionAwareHTable);
  }

  @After
  public void shutdownAfterTest() throws IOException {
    hBaseAdmin.disableTable(TestBytes.table);
    hBaseAdmin.deleteTable(TestBytes.table);
  }

  @Test
  public void testAddTransactionAware() throws Exception {
    FlowManager flowManager = deployFlow(TestFlow.class, ImmutableMap.<String, String>of());

    TimeUnit.SECONDS.sleep(5);

    flowManager.stop();

    TimeUnit.SECONDS.sleep(5);

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();

    // Test that values are written correctly from init, process and destroy.
    byte[] value = result.getValue(TestBytes.family, TestBytes.initQualifier);
    Assert.assertArrayEquals(TestBytes.value, value);

    value = result.getValue(TestBytes.family, TestBytes.processQualifier);
    Assert.assertArrayEquals(TestBytes.value, value);

    value = result.getValue(TestBytes.family, TestBytes.destroyQualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
  }

  /**
   * Test Flow that writes to an instance of TransactionAware.
   */
  public static final class TestFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("testFlow")
        .setDescription("")
        .withFlowlets()
        .add("generator", new GeneratorFlowlet(), 2)
        .add("sink", new SinkFlowlet(), 2)
        .connect()
        .from("generator").to("sink")
        .build();
    }
  }

  private static final class GeneratorFlowlet extends AbstractFlowlet {

    private OutputEmitter<Integer> intEmitter;
    private int i = 0;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      context.addTransactionAware(transactionAwareHTable);

      Put put = new Put(TestBytes.row);
      put.add(TestBytes.family, TestBytes.initQualifier, TestBytes.value);
      transactionAwareHTable.put(put);
    }

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void process() throws Exception {
      Put put = new Put(TestBytes.row);
      put.add(TestBytes.family, TestBytes.processQualifier, TestBytes.value);
      transactionAwareHTable.put(put);

      Integer value = ++i;
      intEmitter.emit(value, "integer", value.hashCode());
    }

    @Override
    public void destroy() {
      Put put = new Put(TestBytes.row);
      put.add(TestBytes.family, TestBytes.destroyQualifier, TestBytes.value);
      try {
        transactionAwareHTable.put(put);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static final class SinkFlowlet extends AbstractFlowlet {

    @Override
    public void initialize(FlowletContext context) throws Exception {
      // no-op
    }

    @HashPartition("integer")
    @ProcessInput
    public void process(Integer value) throws Exception {
      // no-op
    }
  }
}
