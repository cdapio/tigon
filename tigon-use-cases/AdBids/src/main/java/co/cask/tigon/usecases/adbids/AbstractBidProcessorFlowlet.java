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

package co.cask.tigon.usecases.adbids;

import co.cask.tephra.hbase96.TransactionAwareHTable;
import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.Flowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.data.util.hbase.HBaseTableUtil;
import co.cask.tigon.data.util.hbase.HBaseTableUtilFactory;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An abstract {@link Flowlet} that encapsulates the resources needed to access and process bids.
 * In-memory implementations should use the underlying {@link Table} to maintain state. Similarly, distributed
 * implementations must use the {@link TransactionAwareHTable}.
 */
public abstract class AbstractBidProcessorFlowlet extends AbstractFlowlet {
  protected static final Lock TABLE_LOCK = new ReentrantLock();
  protected static final Table<String, String, Integer> AD_BIDS = HashBasedTable.create();
  protected File outputFile;

  protected boolean distributedMode;
  protected TransactionAwareHTable transactionAwareHTable;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    String outputFilePath = context.getRuntimeArguments().get("bids.output.file");
    String hbaseConfFilePath = context.getRuntimeArguments().get("hbase.conf.path");
    if (outputFilePath != null) {
      outputFile = new File(outputFilePath);
    }
    if (hbaseConfFilePath != null) {
      distributedMode = true;
      File hbaseConf = new File(hbaseConfFilePath);
      Configuration configuration = new Configuration(true);
      configuration.addResource(hbaseConf.toURI().toURL());

      HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
      HTableDescriptor hTableDescriptor = new HTableDescriptor(AdBids.BID_TABLE_NAME);
      hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(Item.TRAVEL)));
      hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(Item.SPORTS)));

      HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory().get();
      hBaseTableUtil.createTableIfNotExists(hBaseAdmin, Bytes.toString(AdBids.BID_TABLE_NAME), hTableDescriptor);

      if (!hBaseAdmin.isTableEnabled(AdBids.BID_TABLE_NAME)) {
        hBaseAdmin.enableTable(AdBids.BID_TABLE_NAME);
      }

      hBaseAdmin.close();
      transactionAwareHTable = new TransactionAwareHTable(new HTable(configuration, AdBids.BID_TABLE_NAME));
      context.addTransactionAware(transactionAwareHTable);
    }
  }
}
