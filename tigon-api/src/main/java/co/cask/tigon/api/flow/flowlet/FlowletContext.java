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

package co.cask.tigon.api.flow.flowlet;

import co.cask.tephra.TransactionAware;
import co.cask.tigon.api.RuntimeContext;

/**
 * This interface represents the Flowlet context.
 */
public interface FlowletContext extends RuntimeContext {
  /**
   * @return Number of instances of this flowlet.
   */
  int getInstanceCount();

  /**
   * @return The instance id of this flowlet.
   */
  int getInstanceId();

  /**
   * @return Name of this flowlet.
   */
  String getName();

  /**
   * @return The specification used to configure this {@link Flowlet} instance.
   */
  FlowletSpecification getSpecification();

  /**
   * Add a {@link TransactionAware} to the context.
   * @param transactionAware to add to the context.
   */
  void addTransactionAware(TransactionAware transactionAware);

  /**
   * Add a list of {@link TransactionAware}s to the context.
   * @param transactionAwares to add to the context.
   */
  void addTransactionAwares(Iterable<? extends TransactionAware> transactionAwares);
}
