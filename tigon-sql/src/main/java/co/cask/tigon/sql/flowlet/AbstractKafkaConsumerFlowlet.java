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

package co.cask.tigon.sql.flowlet;

import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.FlowletSpecification;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.Watcher;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractKafkaConsumerFlowlet extends AbstractFlowlet {

  private String zkString;
  private Watcher zkWatcher;
  private Multimap<String, byte[]> authInfos = ImmutableMultimap.of();
  private int sessionTimeout = 10000;

  private static String topic;
  private KafkaClientService kafkaClientService;
  private ZKClientService zkClientService;
  private KafkaConsumer.Preparer kafkaPreparer;
  private int instanceId;
  // TODO
  private long nextOffset = 0;

  protected abstract void create();

  @Override
  public FlowletSpecification configure() {
    create();
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  protected void setTopic(String topic) {
    this.topic = topic;
  }

  protected void setZKString(String zkString) {
    this.zkString = zkString;
  }

  protected void setZKWatcher(Watcher zkWatcher) {
    this.zkWatcher = zkWatcher;
  }

  protected void setAuthInfos(Multimap<String, byte[]> authInfos) {
    this.authInfos = authInfos;
  }

  protected void setSessionTimeout(int sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    Preconditions.checkNotNull(topic, "Kafka topic cannot be null.");
    Preconditions.checkNotNull(zkString, "ZooKeeper connection string cannot be null.");

    super.initialize(context);
    zkClientService = new DefaultZKClientService(zkString, sessionTimeout, zkWatcher, authInfos);
    instanceId = context.getInstanceId();
    kafkaClientService = new ZKKafkaClientService(zkClientService);
    Services.chainStart(zkClientService, kafkaClientService).get();
  }

  @Tick(delay = 200L, unit = TimeUnit.MILLISECONDS)
  public void process() throws Exception {
    kafkaPreparer = kafkaClientService.getConsumer().prepare().add(topic, instanceId, nextOffset);

    final CountDownLatch processLatch = new CountDownLatch(1);
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final Cancellable cancellable = kafkaPreparer.consume(new KafkaConsumer.MessageCallback() {
      @Override
      public void onReceived(Iterator<FetchedMessage> messages) {
        try {
          consume(messages);
          nextOffset = Iterators.getLast(messages).getNextOffset();
          processLatch.countDown();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void finished() {
        finishLatch.countDown();
      }
    });
    // Await with a timeout since in the case that no messages are received, onReceived doesn't count down the latch.
    processLatch.await(1, TimeUnit.SECONDS);
    cancellable.cancel();
    finishLatch.await();
    
    // store the last offset
  }

  protected abstract void consume(Iterator<FetchedMessage> messages) throws Exception;

  @Override
  public void destroy() {
    try {
      Services.chainStop(kafkaClientService, zkClientService).get();
      kafkaClientService.stopAndWait();
      zkClientService.stopAndWait();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
