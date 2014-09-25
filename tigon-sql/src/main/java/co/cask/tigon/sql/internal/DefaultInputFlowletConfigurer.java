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

import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.InputFlowletConfigurer;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import co.cask.tigon.sql.flowlet.InputStreamFormat;
import co.cask.tigon.sql.flowlet.StreamSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Default InputFlowletConfigurer.
 */
public class DefaultInputFlowletConfigurer implements InputFlowletConfigurer {
  private final Map<String, Map.Entry<InputStreamFormat, StreamSchema>> inputStreamSchemas = Maps.newHashMap();
  private final Map<String, String> sqlMap = Maps.newHashMap();
  private String name;
  private String description;

  public DefaultInputFlowletConfigurer(AbstractInputFlowlet flowlet) {
    this.name = flowlet.getClass().getSimpleName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null.");
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addGDATInput(String name, StreamSchema schema) {
    checkInputName(name);
    this.inputStreamSchemas.put(name, new AbstractMap.SimpleEntry<InputStreamFormat, StreamSchema>(
      InputStreamFormat.GDAT, schema));
  }

  @Override
  public void addJSONInput(String name, StreamSchema schema) {
    checkInputName(name);
    this.inputStreamSchemas.put(name, new AbstractMap.SimpleEntry<InputStreamFormat, StreamSchema>(
      InputStreamFormat.JSON, schema));
  }

  @Override
  public void addQuery(String outputName, String sql) {
    Preconditions.checkArgument(outputName != null, "Output Name cannot be null.");
    Preconditions.checkState(!sqlMap.containsKey(outputName), "Output Name already exists.");
    this.sqlMap.put(outputName, sql);
  }

  public InputFlowletSpecification createInputFlowletSpec() {
    return new DefaultInputFlowletSpecification(name, description, inputStreamSchemas, sqlMap);
  }

  private void checkInputName(String name) {
    Preconditions.checkArgument(name != null, "Input Name cannot be null.");
    Preconditions.checkState(!inputStreamSchemas.containsKey(name));
  }
}
