/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.tigon.sql.api.InputStreamFormat;
import co.cask.tigon.sql.api.StreamSchema;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Default InputFlowlet Specification.
 */
public class DefaultInputFlowletSpecification implements InputFlowletSpecification {
  private final String name;
  private final String description;
  private final Map<String, Map.Entry<InputStreamFormat, StreamSchema>> inputSchemas;
  private final Map<String, String> gsql;

  public DefaultInputFlowletSpecification(String name, String description,
                                          Map<String, Map.Entry<InputStreamFormat, StreamSchema>> inputSchemas,
                                          Map<String, String> gsql) {
    this.name = name;
    this.description = description;
    this.inputSchemas = ImmutableMap.copyOf(inputSchemas);
    this.gsql = ImmutableMap.copyOf(gsql);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, Map.Entry<InputStreamFormat, StreamSchema>> getInputSchemas() {
    return inputSchemas;
  }

  @Override
  public Map<String, String> getQuery() {
    return gsql;
  }
}
