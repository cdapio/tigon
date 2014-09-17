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

import co.cask.tigon.api.RuntimeContext;
import co.cask.tigon.api.metrics.Metrics;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.MetricsCollector;
import co.cask.tigon.metrics.MetricsScope;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext implements RuntimeContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;

  private final MetricsCollector programMetrics;

  public AbstractContext(Program program, RunId runId,
                         String metricsContext,
                         MetricsCollectionService metricsCollectionService) {
    this.program = program;
    this.runId = runId;

    if (metricsCollectionService != null) {
      // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
      this.programMetrics = metricsCollectionService.getCollector(MetricsScope.REACTOR, metricsContext, "0");
    } else {
      this.programMetrics = null;
    }
  }

  public abstract Metrics getMetrics();

  @Override
  public String toString() {
    return String.format("program=%s, runid=%s", getProgramName(), runId);
  }

  public MetricsCollector getProgramMetrics() {
    return programMetrics;
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  /**
   * Release all resources held by this context. Subclasses should override this method to release additional resources.
   */
  public void close() {

  }
}
