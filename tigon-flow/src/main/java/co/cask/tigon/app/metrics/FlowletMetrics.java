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

package co.cask.tigon.app.metrics;


import co.cask.tigon.metrics.MetricsCollectionService;
import co.cask.tigon.metrics.MetricsScope;

/**
 * Metrics collector for Flowlet.
 */
public class FlowletMetrics extends AbstractProgramMetrics {

  public FlowletMetrics(MetricsCollectionService collectionService, String flowId, String flowletId) {
    // No support runID for now.
    super(collectionService.getCollector(
      MetricsScope.USER, String.format("%s.%s", flowId, flowletId), "0"));
  }
}
