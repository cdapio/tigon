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

import co.cask.tigon.api.annotation.Output;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.api.flow.flowlet.Flowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * A {@link Flowlet} that notifies advertisers that a new user-id may be bid on. In addition, it also provides
 * advertisers relevant information needed to make a bid.
 */
public final class AdvertiserNotificationFlowlet extends AbstractBidProcessorFlowlet {

  @Output(Item.TRAVEL)
  private OutputEmitter<IdData> travelOutputEmitter;

  @Output(Item.SPORTS)
  private OutputEmitter<IdData> sportsOutputEmitter;

  @ProcessInput
  @SuppressWarnings("UnusedDeclaration")
  public void process(String id) throws Exception {
    if (distributedMode) {
      distributedProcess(id);
    } else {
      inMemoryProcess(id);
    }
  }

  private void distributedProcess(String id) throws Exception {
    int travelBids = 0;
    int sportsBids = 0;

    Result result = transactionAwareHTable.get(new Get(Bytes.toBytes(id)));
    if (!result.isEmpty()) {
      travelBids = result.getFamilyMap(Bytes.toBytes(Item.TRAVEL)).size();
      sportsBids = result.getFamilyMap(Bytes.toBytes(Item.SPORTS)).size();
    }

    int totalBids = travelBids + sportsBids;
    sportsOutputEmitter.emit(new IdData(id, sportsBids, totalBids));
    travelOutputEmitter.emit(new IdData(id, travelBids, totalBids));
  }

  private void inMemoryProcess(String id) throws Exception {
    TABLE_LOCK.lock();
    try {
      if (!AD_BIDS.containsRow(id)) {
        for (Field item : Item.class.getDeclaredFields()) {
          AD_BIDS.put(id, (String) item.get(null), 0);
        }
      }

      int totalItems = 0;
      Map<String, Integer> idItems = AD_BIDS.row(id);
      for (int itemCount : idItems.values()) {
        totalItems += itemCount;
      }

      sportsOutputEmitter.emit(new IdData(id, AD_BIDS.get(id, Item.SPORTS), totalItems));
      travelOutputEmitter.emit(new IdData(id, AD_BIDS.get(id, Item.TRAVEL), totalItems));
    } finally {
      TABLE_LOCK.unlock();
    }
  }
}
