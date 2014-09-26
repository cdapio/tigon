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

package co.cask.tigon.runtime;

import com.google.inject.Module;

/**
 * Runtime Module defines all of the methods that all of our Guice modules must
 * implement. We expect all modules that are found in each component's "runtime"
 * package to extend this class.
 */
public abstract class RuntimeModule {

  /**
   * Get a combined Module that includes all of the modules and classes
   * required to instantiate and run an in memory instance of Tigon.
   *
   * @return A combined set of Modules required for InMemory execution.
   */
  public abstract Module getInMemoryModules();

  /**
   * Get a combined Module that includes all of the modules and classes
   * required to instantiate and run a single node instance of Tigon.
   *
   * @return A combined set of Modules required for SingleNode execution.
   */
  public abstract Module getSingleNodeModules();

  /**
   * Get a combined Module that includes all of the modules and classes
   * required to instantiate and run a fully distributed instance of Tigon.
   *
   * @return A combined set of Modules required for distributed execution.
   */
  public abstract Module getDistributedModules();

}
