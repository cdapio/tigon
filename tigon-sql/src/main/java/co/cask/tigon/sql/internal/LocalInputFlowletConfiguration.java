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

package co.cask.tigon.sql.internal;

import co.cask.tigon.sql.flowlet.InputFlowletConfiguration;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import org.apache.twill.filesystem.Location;

/**
 * Sets up LocalInputFlowlet {@link co.cask.tigon.sql.flowlet.InputFlowletSpecification}
 * in a given Directory location.
 */
public class LocalInputFlowletConfiguration implements InputFlowletConfiguration {
  private Location dir;
  private InputFlowletSpecification spec;

  public LocalInputFlowletConfiguration(Location dir, InputFlowletSpecification spec) {
    this.dir = dir;
    this.spec = spec;
  }

  @Override
  public Location createStreamEngineProcesses() {
    StreamBinaryGenerator binaryGenerator = new StreamBinaryGenerator(dir, spec);
    return binaryGenerator.createStreamProcesses();
  }
}
