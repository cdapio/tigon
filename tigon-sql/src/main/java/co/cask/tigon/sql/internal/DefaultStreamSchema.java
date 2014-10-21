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

import co.cask.tigon.sql.flowlet.GDATField;
import co.cask.tigon.sql.flowlet.StreamSchema;

import java.util.List;

/**
 * Default StreamSchema.
 */
public class DefaultStreamSchema implements StreamSchema {
  private List<GDATField> fields;
  private String name;

  public DefaultStreamSchema(String name, List<GDATField> fields) {
    this.name = name;
    this.fields = fields;
  }

  @Override
  public List<GDATField> getFields() {
    return fields;
  }

  @Override
  public String getName() {
    return name;
  }
}
