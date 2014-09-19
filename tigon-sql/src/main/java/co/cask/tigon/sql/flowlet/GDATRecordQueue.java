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

package co.cask.tigon.sql.flowlet;

import co.cask.tigon.sql.io.GDATDecoder;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;

/**
 * GDATRecordQueue
 * Queue of the incoming GDAT Records that is used by the AbstractInputFlowlet
 */
public class GDATRecordQueue {
  private final Queue<Map.Entry<String, GDATDecoder>> dataQueue;
  private final Collection<Map.Entry<String, GDATDecoder>> pendingRecords;

  /**
   * Constructor
   */
  public GDATRecordQueue() {
    dataQueue = Queues.newConcurrentLinkedQueue();
    pendingRecords = Lists.newArrayList();
  }

  /**
   * This function returns the next entry in the data queue and adds it to a queue of pending items. This pendingRecords
   * serves as a place holder for all the items of the current transaction.
   * @return The next entry in the data queue.
   */
  public Map.Entry<String, GDATDecoder> getNext() {
    Map.Entry<String, GDATDecoder> element = dataQueue.poll();
    pendingRecords.add(element);
    return element;
  }

  /**
   * Add another entry to the end of the data queue
   * @param record The map entry of queryName and the {@link co.cask.tigon.sql.io.GDATDecoder} object to be
   *               added at the end of the queue
   */
  public void add(Map.Entry<String, GDATDecoder> record) {
    dataQueue.add(record);
  }

  /**
   * Removes the elements that have already been returned by getNext()
   * This method is called when the transaction has been completed successfully
   */
  public void commit() {
    pendingRecords.clear();
  }

  /**
   * Checks if the data queue is empty
   * @return Boolean true or false depending on the number of elements in the queue
   */
  public boolean isEmpty() {
    return dataQueue.isEmpty();
  }

  /**
   * Rolls back the uncommitted data records to the original data queue.
   * Note: This function does not guarantee preservation of data record order.
   */
  public void rollback() {
    dataQueue.addAll(pendingRecords);
    pendingRecords.clear();
  }
}
