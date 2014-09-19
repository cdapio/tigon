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

package co.cask.tigon.sql.internal;

import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.GDATField;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.StreamSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Create Schema String from {@link co.cask.tigon.sql.flowlet.StreamSchema} and vice versa.
 */
public class StreamSchemaCodec {

  /**
   * Sample Schema String:
   *     ullong timestamp get_gdat_ullong_pos1 (increasing);
   *     uint istream get_gdata_uint_pos2;
   */
  public static String serialize(StreamSchema schema) {
    StringBuilder builder = new StringBuilder();
    int fieldCount = 1;
    for (GDATField field : schema.getFields()) {
      builder.append(field.getType().getTypeName()).append(" ").append(field.getName()).append(" get_gdat_")
        .append(field.getType().getTypeName()).append("_pos").append(fieldCount);
      builder.append(field.getSlidingWindowType().getAttribute());
      builder.append(";").append(Constants.NEWLINE);
      fieldCount++;
    }
    return builder.toString();
  }

  /**
   * Generate {@link co.cask.tigon.sql.flowlet.StreamSchema} from serialized Schema String.
   */
  public static StreamSchema deserialize(String schemaString) {
    Iterable<String> fieldArray = Splitter.on(';').trimResults().omitEmptyStrings().split(schemaString);
    StreamSchema.Builder builder = new StreamSchema.Builder();
    for (String field : fieldArray) {
      Iterable<String> defnArray = Splitter.onPattern("\\s+").trimResults().omitEmptyStrings().split(field);
      List<String> defnList = Lists.newArrayList(defnArray);
      Preconditions.checkArgument(defnList.size() >= 3);
      GDATFieldType type = GDATFieldType.getGDATFieldType(defnList.get(0).toUpperCase());
      String fieldName = defnList.get(1);
      builder.addField(fieldName, type);
    }
    return builder.build();
  }
}

