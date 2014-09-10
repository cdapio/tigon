/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.tigon.sql.api;

import co.cask.tigon.sql.io.GDATDecoder;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * GDATRecordQueueTest
 */
public class GDATRecordQueueTest {
  private static GDATRecordQueue recordQueue;

  @BeforeClass
  public static void setup() {
    recordQueue = new GDATRecordQueue();
  }

  @Test
  public void testQueue() {
    Assert.assertTrue(recordQueue.isEmpty());
    for (int i = 0; i < 5; i++) {
      Map.Entry<String, GDATDecoder> record = Maps.immutableEntry(i + "", null);
      recordQueue.add(record);
    }
    Assert.assertTrue(recordQueue.getNext().getKey().equals("0"));
    Assert.assertTrue(recordQueue.getNext().getKey().equals("1"));
    Assert.assertTrue(recordQueue.getNext().getKey().equals("2"));
    Assert.assertTrue(!recordQueue.isEmpty());
    recordQueue.commit();
    Assert.assertTrue(!recordQueue.isEmpty());
    Assert.assertTrue(recordQueue.getNext().getKey().equals("3"));
    Assert.assertTrue(recordQueue.getNext().getKey().equals("4"));
    Assert.assertTrue(recordQueue.isEmpty());
    recordQueue.rollback();
    Assert.assertTrue(!recordQueue.isEmpty());
    Assert.assertTrue(recordQueue.getNext().getKey().equals("3"));
    Assert.assertTrue(recordQueue.getNext().getKey().equals("4"));
    recordQueue.commit();
    Assert.assertTrue(recordQueue.isEmpty());
  }

  @Test
  public void testMultiThreadedAddOperation() {
    Assert.assertTrue(recordQueue.isEmpty());
    final long recordAddCount = 100000;
    int threadCount = 6;
    final CountDownLatch addLatch = new CountDownLatch(1);
    final CountDownLatch feedLatch = new CountDownLatch(threadCount);

    class AddRecords implements Runnable {
      @Override
      public void run() {
        try {
          // Wait for latch to countdown to zero or ten seconds whichever is sooner
          addLatch.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          Throwables.propagate(e);
        }
        for (int i = 1; i <= recordAddCount; i++) {
          Map.Entry<String, GDATDecoder> record = Maps.immutableEntry(i + "", null);
          recordQueue.add(record);
        }
        feedLatch.countDown();
      }
    }

    // Parallel add operations to the recordQueue
    for (int threadCounter = 0; threadCounter < threadCount; threadCounter++) {
      new Thread(new AddRecords()).start();
    }
    addLatch.countDown();

    int numberOfRecords = 0;
    long sumOfRecordValues = 0;
    try {
      // Wait for latch to countdown to zero or ten seconds whichever is sooner
      feedLatch.await(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    while (!recordQueue.isEmpty()) {
      sumOfRecordValues += Long.parseLong(recordQueue.getNext().getKey());
      numberOfRecords++;
    }
    Assert.assertTrue(recordQueue.isEmpty());
    recordQueue.rollback();
    Assert.assertTrue(!recordQueue.isEmpty());
    Assert.assertEquals(recordAddCount * threadCount, numberOfRecords);
    Assert.assertEquals((recordAddCount * (recordAddCount + 1)) * threadCount / 2, sumOfRecordValues);
  }
}
