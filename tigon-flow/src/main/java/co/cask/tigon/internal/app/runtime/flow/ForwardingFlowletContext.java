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

import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.FlowletSpecification;

import java.util.Map;

/**
 * A forwarding {@link FlowletContext} that delegates to the underlying instance.
 */
public abstract class ForwardingFlowletContext implements FlowletContext {
  protected final FlowletContext delegate;

  /**
   * Create a FlowletContext that delegates to the underlying delegate.
   * @param flowletContext to delegate to.
   */
  public ForwardingFlowletContext(FlowletContext flowletContext) {
    this.delegate = flowletContext;
  }

  @Override
  public int getInstanceCount() {
    return delegate.getInstanceCount();
  }

  @Override
  public int getInstanceId() {
    return delegate.getInstanceId();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public FlowletSpecification getSpecification() {
    return delegate.getSpecification();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return delegate.getRuntimeArguments();
  }
}
