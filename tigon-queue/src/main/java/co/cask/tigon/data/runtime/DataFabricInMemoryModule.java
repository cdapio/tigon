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
package co.cask.tigon.data.runtime;

import com.continuuity.tephra.metrics.TxMetricsCollector;
import com.continuuity.tephra.runtime.TransactionModules;
import co.cask.tigon.data.queue.QueueClientFactory;
import co.cask.tigon.data.transaction.metrics.TransactionManagerMetricsCollector;
import co.cask.tigon.data.transaction.queue.QueueAdmin;
import co.cask.tigon.data.transaction.queue.inmemory.InMemoryQueueAdmin;
import co.cask.tigon.data.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

/**
 * The Guice module of data fabric bindings for in memory execution.
 */
public class DataFabricInMemoryModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind TxDs2 stuff

    bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);

    // bind transactions
    bind(TxMetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
    install(new TransactionModules().getInMemoryModules());
  }
}
