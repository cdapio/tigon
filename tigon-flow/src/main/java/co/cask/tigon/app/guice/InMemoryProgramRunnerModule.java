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

package co.cask.tigon.app.guice;

import co.cask.tigon.conf.CConfiguration;
import co.cask.tigon.internal.app.queue.QueueReaderFactory;
import co.cask.tigon.internal.app.runtime.ProgramRunner;
import co.cask.tigon.internal.app.runtime.ProgramRunnerFactory;
import co.cask.tigon.internal.app.runtime.flow.FlowProgramRunner;
import co.cask.tigon.internal.app.runtime.flow.FlowletProgramRunner;
import co.cask.tigon.logging.common.LocalLogWriter;
import co.cask.tigon.logging.common.LogWriter;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 *
 */
final class InMemoryProgramRunnerModule extends PrivateModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    // Bind and expose LogWriter (a bit hacky, but needed by MapReduce for now)
    bind(LogWriter.class).to(LocalLogWriter.class);
    expose(LogWriter.class);

    // Bind ServiceAnnouncer for procedure.
    bind(ServiceAnnouncer.class).to(DiscoveryServiceAnnouncer.class);

    // For Binding queue stuff
    bind(QueueReaderFactory.class).in(Scopes.SINGLETON);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(FlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOWLET).to(FlowletProgramRunner.class);

    bind(ProgramRunnerFactory.class).to(InMemoryFlowProgramRunnerFactory.class).in(Scopes.SINGLETON);
    // Note: Expose for test cases. Need to refactor test cases.
    expose(ProgramRunnerFactory.class);

    // For binding DataSet transaction stuff
    install(new DataFabricFacadeModule());
  }

  @Singleton
  @Provides
  private LocalLogWriter providesLogWriter(CConfiguration configuration) {
    return new LocalLogWriter(configuration);
  }

  @Singleton
  private static final class InMemoryFlowProgramRunnerFactory implements ProgramRunnerFactory {

    private final Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers;

    @Inject
    private InMemoryFlowProgramRunnerFactory(Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers) {
      this.providers = providers;
    }

    @Override
    public ProgramRunner create(ProgramRunnerFactory.Type programType) {
      Provider<ProgramRunner> provider = providers.get(programType);
      Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
      return provider.get();
    }
  }

  @Singleton
  private static final class DiscoveryServiceAnnouncer implements ServiceAnnouncer {

    private final DiscoveryService discoveryService;

    @Inject
    private DiscoveryServiceAnnouncer(DiscoveryService discoveryService) {
      this.discoveryService = discoveryService;
    }

    @Override
    public Cancellable announce(final String serviceName, final int port) {
      return discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return serviceName;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return new InetSocketAddress(port);
        }
      });
    }
  }
}
