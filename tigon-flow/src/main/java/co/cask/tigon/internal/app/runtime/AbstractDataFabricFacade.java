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

package co.cask.tigon.internal.app.runtime;

import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tigon.data.queue.ConsumerConfig;
import co.cask.tigon.data.queue.QueueClientFactory;
import co.cask.tigon.data.queue.QueueConsumer;
import co.cask.tigon.data.queue.QueueName;
import co.cask.tigon.data.queue.QueueProducer;
import co.cask.tigon.data.transaction.queue.QueueMetrics;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Set;

/**
 * Abstract base class for implementing DataFabricFacade.
 */
public abstract class AbstractDataFabricFacade implements DataFabricFacade {

  private final QueueClientFactory queueClientFactory;
  private final TransactionSystemClient txSystemClient;
  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();

  public AbstractDataFabricFacade(TransactionSystemClient txSystemClient, QueueClientFactory queueClientFactory) {
    this.txSystemClient = txSystemClient;
    this.queueClientFactory = queueClientFactory;
  }

  @Override
  public TransactionContext createTransactionManager() {
    return new TransactionContext(txSystemClient, Iterables.unmodifiableIterable(txAware));
  }

  @Override
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, numGroups);
    if (consumer instanceof TransactionAware) {
      consumer = new CloseableQueueConsumer(this, consumer);
      txAware.add((TransactionAware) consumer);
    }
    return consumer;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    QueueProducer producer = queueClientFactory.createProducer(queueName, queueMetrics);
    if (producer instanceof TransactionAware) {
      txAware.add((TransactionAware) producer);
    }
    return producer;
  }

  public void removeTransactionAware(TransactionAware transactionAware) {
    txAware.remove(transactionAware);
  }
}
