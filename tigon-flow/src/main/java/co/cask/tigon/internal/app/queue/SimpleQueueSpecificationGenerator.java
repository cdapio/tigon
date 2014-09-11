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

package co.cask.tigon.internal.app.queue;

import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.FlowletConnection;
import co.cask.tigon.api.flow.FlowletDefinition;
import co.cask.tigon.app.program.Id;
import co.cask.tigon.app.queue.QueueSpecification;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.Map;
import java.util.Set;

/**
 * Concrete implementation of {@link co.cask.tigon.app.queue.QueueSpecificationGenerator} for generating queue
 * names.
 */
public final class SimpleQueueSpecificationGenerator extends AbstractQueueSpecificationGenerator {

  /**
   * Account Name under which the stream names to generated.
   */
  private final Id.Application appId;

  /**
   * Constructor that takes an appId.
   *
   * @param appId under which the stream is represented.
   */
  public SimpleQueueSpecificationGenerator(Id.Application appId) {
    this.appId = appId;
  }

  /**
   * Given a {@link FlowSpecification}.
   *
   * @param input {@link FlowSpecification}
   * @return A {@link com.google.common.collect.Table}
   */
  @Override
  public Table<Node, String, Set<QueueSpecification>> create(FlowSpecification input) {
    Table<Node, String, Set<QueueSpecification>> table = HashBasedTable.create();

    String flow = input.getName();
    Map<String, FlowletDefinition> flowlets = input.getFlowlets();

    // Iterate through connections of a flow.
    for (FlowletConnection connection : input.getConnections()) {
      final String source = connection.getSourceName();
      final String target = connection.getTargetName();
      final Node sourceNode = new Node(connection.getSourceType(), source);

      Set<QueueSpecification> queueSpec = generateQueueSpecification(
        appId, flow, connection, flowlets.get(target).getInputs(), flowlets.get(source).getOutputs());

      Set<QueueSpecification> queueSpecifications = table.get(sourceNode, target);
      if (queueSpecifications == null) {
        queueSpecifications = Sets.newHashSet();
        table.put(sourceNode, target, queueSpecifications);
      }
      queueSpecifications.addAll(queueSpec);
    }
    return table;
  }
}
