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

package co.cask.tigon.internal.app.runtime.flow;

import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.FlowletSpecification;
import co.cask.tigon.api.metrics.Metrics;
import co.cask.tigon.app.metrics.FlowletMetrics;
import co.cask.tigon.app.program.Program;
import co.cask.tigon.internal.app.runtime.AbstractContext;
import co.cask.tigon.internal.app.runtime.Arguments;
import co.cask.tigon.internal.app.runtime.DataFabricFacade;
import co.cask.tigon.logging.FlowletLoggingContext;
import co.cask.tigon.logging.LoggingContext;
import co.cask.tigon.metrics.MetricsCollectionService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;

import java.util.List;
import java.util.Map;

/**
 * Internal implementation of {@link FlowletContext}.
 */
final class BasicFlowletContext extends AbstractContext implements FlowletContext {

  private final String flowId;
  private final String flowletId;
  private final long groupId;
  private final int instanceId;
  private final FlowletSpecification flowletSpec;

  private volatile int instanceCount;
  private final FlowletMetrics flowletMetrics;
  private final Arguments runtimeArguments;
  private final List<TransactionAware> transactionAwares;
  private final DataFabricFacade dataFabricFacade;
  private TransactionContext transactionContext;
  private final ServiceAnnouncer serviceAnnouncer;

  BasicFlowletContext(Program program, String flowletId,
                      int instanceId, RunId runId,
                      int instanceCount,
                      Arguments runtimeArguments, FlowletSpecification flowletSpec,
                      MetricsCollectionService metricsCollectionService, DataFabricFacade dataFabricFacade,
                      ServiceAnnouncer serviceAnnouncer) {
    super(program, runId, getMetricContext(program, flowletId, instanceId), metricsCollectionService);
    this.flowId = program.getName();
    this.flowletId = flowletId;
    this.groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.runtimeArguments = runtimeArguments;
    this.flowletSpec = flowletSpec;
    this.flowletMetrics = new FlowletMetrics(metricsCollectionService, flowId, flowletId);
    this.transactionAwares = Lists.newArrayList();
    this.dataFabricFacade = dataFabricFacade;
    this.serviceAnnouncer = serviceAnnouncer;
  }

  @Override
  public String toString() {
    return String.format("flowlet=%s, instance=%d, groupsize=%s, %s",
                         getFlowletId(), getInstanceId(), getInstanceCount(), super.toString());
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public String getName() {
    return getFlowletId();
  }

  @Override
  public FlowletSpecification getSpecification() {
    return flowletSpec;
  }

  @Override
  public void addTransactionAware(TransactionAware transactionAware) {
    transactionAwares.add(transactionAware);
    if (transactionContext != null) {
      transactionContext.addTransactionAware(transactionAware);
    }
  }

  @Override
  public void addTransactionAwares(Iterable<? extends TransactionAware> transactionAwares) {
    Iterables.addAll(this.transactionAwares, transactionAwares);
    if (transactionContext != null) {
      for (TransactionAware transactionAware : transactionAwares) {
        transactionContext.addTransactionAware(transactionAware);
      }
    }
  }

  /**
   * @return A map of runtime key and value arguments supplied by the user.
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : runtimeArguments) {
      builder.put(entry);
    }
    return builder.build();
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  public String getFlowId() {
    return flowId;
  }

  public String getFlowletId() {
    return flowletId;
  }

  /**
   * Create a new {@link TransactionContext} for this flowlet. Add all {@link TransactionAware}s to the context.
   * @return a new TransactionContext.
   */
  public TransactionContext createTransactionContext() {
    transactionContext = dataFabricFacade.createTransactionManager();
    for (TransactionAware transactionAware : transactionAwares) {
      this.transactionContext.addTransactionAware(transactionAware);
    }
    return transactionContext;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getFlowId(), getFlowletId());
  }

  @Override
  public Metrics getMetrics() {
    return flowletMetrics;
  }

  public long getGroupId() {
    return groupId;
  }

  private static String getMetricContext(Program program, String flowletId, int instanceId) {
    return String.format("%s.%s.%d", program.getName(), flowletId, instanceId);
  }

  @Override
  public Cancellable announce(String s, int i) {
    return serviceAnnouncer.announce(s, i);
  }
}
