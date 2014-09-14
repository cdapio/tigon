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

import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.NoOpMetricsCollectionService;
import co.cask.tigon.runtime.RuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 *
 */
public final class MetricsClientRuntimeModule extends RuntimeModule {

  //TODO: Add MetricsHandlers!

  @Override
  public Module getInMemoryModules() {
    return getNoopModules();
  }

  @Override
  public Module getSingleNodeModules() {
    return getNoopModules();
  }

  @Override
  public Module getDistributedModules() {
    return getNoopModules();
  }

  /**
   * Returns a module that bind MetricsCollectionService to a noop one.
   */
  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
      }
    };
  }
}
