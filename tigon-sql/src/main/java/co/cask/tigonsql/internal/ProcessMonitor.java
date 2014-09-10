/**
 * Copyright 2012-2014 Cask, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tigonsql.internal;

import java.util.Set;

/**
 * Class used by the {@link HealthInspector} to notify failure
 */
public interface ProcessMonitor {
  /**
   * Method invoked whenever the {@link HealthInspector} fails to detect heartbeat.
   *
   * @param errorProcessNames Set of all processes for which no heartbeat was detected
   */
  public void notifyFailure(Set<String> errorProcessNames);
}
