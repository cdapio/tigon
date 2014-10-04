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

import co.cask.tigon.api.annotation.HashPartition;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.api.flow.flowlet.Flowlet;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Flowlet} that grants bids to advertisers for user-ids. It waits for a specific amount of time to allow
 * all advertisers to register their bid before granting it to the advertiser with the maximum bid.
 */
public final class GrantBidsFlowlet extends AbstractBidProcessorFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(GrantBidsFlowlet.class);
  private static final int BID_WAIT_TIME_MS = 5000;
  private static final Gson GSON = new Gson();
  private static final Multimap<String, Bid> bids = Multimaps.synchronizedListMultimap(
                                                    ArrayListMultimap.<String, Bid>create());
  private static final DelayQueue<DelayedId> idDelayQueue = new DelayQueue<DelayedId>();

  @SuppressWarnings("UnusedDeclaration")
  @ProcessInput
  @HashPartition("userId")
  public void process(Bid bid) throws Exception {
    bids.put(bid.getId(), bid);
    DelayedId delayedId = new DelayedId(bid.getId());
    if (!idDelayQueue.contains(delayedId)) {
      idDelayQueue.put(delayedId);
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  @Tick(delay = 1L, unit = TimeUnit.SECONDS)
  public void grantBid() throws Exception {
    DelayedId delayedId = idDelayQueue.poll();
    while (delayedId != null) {
      String id = delayedId.getId();
      Bid maxBid;
      try {
        maxBid = Collections.max(bids.get(id), new Comparator<Bid>() {
          @Override
          public int compare(Bid bid, Bid bid2) {
            if (bid.getAmount() == bid2.getAmount()) {
              return 0;
            } else {
              return (bid.getAmount() > bid2.getAmount() ? 1 : -1);
            }
          }
        });
      } catch (NoSuchElementException e) {
        maxBid = null;
      }

      bids.removeAll(id);
      if (maxBid != null) {
        if (distributedMode) {
          distributedGrantBid(maxBid);
        } else {
          inMemoryGrantBid(maxBid);
        }
        LOG.info("Bid for {} granted to {} for {}.", id, maxBid.getItem(), maxBid.getAmount());
      }
      bids.removeAll(id);
      delayedId = idDelayQueue.poll();
    }
  }

  private void inMemoryGrantBid(Bid bid) throws Exception {
    TABLE_LOCK.lock();
    try {
      String item = bid.getItem();
      int value = AD_BIDS.get(bid.getId(), item) + 1;
      AD_BIDS.put(bid.getId(), item, value);
    } finally {
      TABLE_LOCK.unlock();
    }
    if (outputFile != null) {
      Files.append(GSON.toJson(bid, Bid.class), outputFile, Charsets.UTF_8);
      Files.append("\n", outputFile, Charsets.UTF_8);
    }
  }

  private void distributedGrantBid(Bid bid) throws Exception {
    Put put = new Put(Bytes.toBytes(bid.getId()));
    put.add(Bytes.toBytes(bid.getItem()), Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(bid.getAmount()));
    transactionAwareHTable.put(put);
  }

  /**
   * A representation of a user-id that, via an instance of {@link DelayQueue}, forces them to be accessible strictly
   * after a specific amount of time.
   */
  private final class DelayedId implements Delayed {
    private final long startTime;
    private final String id;

    DelayedId(String id) {
      this.startTime = System.currentTimeMillis();
      this.id = id;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      long diff = BID_WAIT_TIME_MS - (System.currentTimeMillis() - startTime);
      return timeUnit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
      if (this.getDelay(TimeUnit.MILLISECONDS) == delayed.getDelay(TimeUnit.MILLISECONDS)) {
        return 0;
      }
      return ((this.getDelay(TimeUnit.MILLISECONDS) > delayed.getDelay(TimeUnit.MILLISECONDS))) ? 1 : -1;
    }

    String getId() {
      return id;
    }

    @Override
    public int hashCode() {
      return this.id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof DelayedId && this.id.equals(((DelayedId) o).getId());
    }
  }
}
