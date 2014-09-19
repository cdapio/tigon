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

package co.cask.tigon.sql.util;

import co.cask.tigon.sql.flowlet.GDATField;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.io.GDATEncoder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * GDAT format conversion utility.
 */
public class GDATFormatUtil {

  public static int getGDATFieldSize(StreamSchema schema) {
    int size = 0;
    for (GDATField field : schema.getFields()) {
      size += field.getType().getSize();
    }
    return size;
  }

  public static void encodeEOF(StreamSchema schema, OutputStream out) throws IOException {
    List<String> values = Lists.newArrayList();
    for (GDATField field : schema.getFields()) {
      if (field.getType() != GDATFieldType.STRING) {
        values.add("0");
      } else {
        values.add("");
      }
    }
    encode(values, schema, out, true);
  }

  //Utility method that encodes a given Schema and list of String values to GDAT format and writes to an OutputStream.
  //TODO: Think about using Json objects to parse the JSON input instead of considering all the values as Strings.
  public static void encode(List<String> valueList, StreamSchema schema, OutputStream out, boolean eof)
    throws IOException {
    int index = 0;
    GDATEncoder encoder = new GDATEncoder();
    Preconditions.checkArgument(valueList.size() == schema.getFields().size());
    for (GDATField field : schema.getFields()) {
      String value = valueList.get(index);
      switch(field.getType()) {
        case BOOL:
          encoder.writeBool(Boolean.parseBoolean(value));
          break;
        case INT:
          encoder.writeInt(Integer.valueOf(value));
          break;
        case LONG:
          encoder.writeLong(Long.valueOf(value));
          break;
        case DOUBLE:
          encoder.writeDouble(Double.valueOf(value));
          break;
        case STRING:
          encoder.writeString(value);
          break;
      }
      index++;
    }
    if (!eof) {
      encoder.writeTo(out);
    } else {
      encoder.writeEOFRecord(out);
    }
  }

  //Given a gdatHeader extract serialized Schema String.
  public static String getSerializedSchema(String gdatHeader) {
    /*Format :
       GDAT\nVERSION:4\nSCHEMALENGTH:123\n
       FTA {\n
         STREAM <name> {\n
           <list of fields>\n
         }\n
         <query text>\n
       }
     */
    int startBracketIndex = gdatHeader.indexOf("{");
    startBracketIndex = gdatHeader.indexOf("{", startBracketIndex + 1);
    int endBracketIndex = gdatHeader.indexOf("}");
    Preconditions.checkArgument(endBracketIndex > startBracketIndex);
    return gdatHeader.substring(startBracketIndex + 1, endBracketIndex);
  }
}
