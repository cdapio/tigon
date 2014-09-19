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

package co.cask.tigon.flow.test;

import co.cask.tigon.internal.app.runtime.ProgramController;
import co.cask.tigon.internal.app.runtime.ProgramOptionConstants;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Default Flow Manager.
 */
public class DefaultFlowManager implements FlowManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFlowManager.class);
  private final ProgramController controller;

  public DefaultFlowManager(ProgramController controller) {
    this.controller = controller;
  }

  @Override
  public void setFlowletInstances(String flowletName, int instances) {
    Map<String, String> instanceOptions = Maps.newHashMap();
    instanceOptions.put("flowlet", flowletName);
    instanceOptions.put("newInstances", String.valueOf(instances));
    controller.command(ProgramOptionConstants.FLOWLET_INSTANCES, instanceOptions);
  }

  @Override
  public void stop() {
    try {
      controller.stop().get();
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }
}
