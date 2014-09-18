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

package co.cask.tigon.internal.app.runtime.distributed;

import com.continuuity.tephra.TransactionManager;
import com.continuuity.tephra.distributed.TransactionService;
import com.continuuity.tephra.persist.TransactionStateStorage;
import com.continuuity.tephra.runtime.TransactionStateStorageProvider;
import co.cask.tigon.app.guice.MetricsClientRuntimeModule;
import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.conf.Constants;
import co.cask.tigon.data.runtime.DataFabricModules;
import co.cask.tigon.data.runtime.HDFSTransactionStateStorageProvider;
import co.cask.tigon.data.runtime.TransactionManagerProvider;
import co.cask.tigon.guice.ConfigModule;
import co.cask.tigon.guice.DiscoveryRuntimeModule;
import co.cask.tigon.guice.IOModule;
import co.cask.tigon.guice.LocationRuntimeModule;
import co.cask.tigon.guice.ZKClientModule;
import co.cask.tigon.metrics.MetricsCollectionService;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TwillRunnable to run Transaction Service through twill.
 */
public class TransactionServiceTwillRunnable extends AbstractMasterTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceTwillRunnable.class);

  private ZKClientService zkClient;
  private MetricsCollectionService metricsCollectionService;
  private TransactionService txService;

  public TransactionServiceTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    try {
      getCConfiguration().set(Constants.Transaction.Container.ADDRESS, context.getHost().getCanonicalHostName());

      Injector injector = createGuiceInjector(getCConfiguration(), getConfiguration());

      LOG.info("Initializing runnable {}", name);
      // Set the hostname of the machine so that cConf can be used to start internal services
      LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());


      //Get Zookeeper Client Instances
      zkClient = injector.getInstance(ZKClientService.class);

      // Get the metrics collection service
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

      // Get the Transaction Service
      txService = injector.getInstance(TransactionService.class);

      LOG.info("Runnable initialized {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void getServices(List<? super Service> services) {
    services.add(zkClient);
    services.add(metricsCollectionService);
    services.add(txService);
  }

  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      createDataFabricModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules()
    );
  }

  private static Module createDataFabricModule() {
    return Modules.override(new DataFabricModules().getDistributedModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Bind to provider that create new instances of storage and tx manager every time.
        bind(TransactionStateStorage.class).annotatedWith(Names.named("persist"))
          .toProvider(HDFSTransactionStateStorageProvider.class);
        bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class);
        bind(TransactionManager.class).toProvider(TransactionManagerProvider.class);
      }
    });
  }
}
