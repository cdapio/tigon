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

package co.cask.tigonsql.internal;

import co.cask.tigonsql.api.GDATField;
import co.cask.tigonsql.api.StreamSchema;

import java.util.List;

/**
 * Default StreamSchema.
 */
public class DefaultStreamSchema implements StreamSchema {
  private List<GDATField> fields;

  public DefaultStreamSchema(List<GDATField> fields) {
    this.fields = fields;
  }

  @Override
  public List<GDATField> getFields() {
    return fields;
  }

}
