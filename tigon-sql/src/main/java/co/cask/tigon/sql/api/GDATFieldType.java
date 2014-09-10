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

package co.cask.tigon.sql.api;

import com.continuuity.internal.io.Schema;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * PrimitiveTypes used in the GDAT format.
 */
public enum GDATFieldType {
  BOOL("bool", Ints.BYTES, Schema.of(Schema.Type.BOOLEAN)),
  INT("int", Ints.BYTES, Schema.of(Schema.Type.INT)),
  LONG("llong", Longs.BYTES, Schema.of(Schema.Type.LONG)),
  DOUBLE("float", Doubles.BYTES, Schema.of(Schema.Type.DOUBLE)),
  STRING("v_str", 3 * Ints.BYTES, Schema.of(Schema.Type.STRING));

  private String type;
  private int size;
  private Schema schema;

  //Field Type.
  private GDATFieldType(String type, int size, Schema schema) {
    this.type = type;
    this.size = size;
    this.schema = schema;
  }

  /**
   * Return the field type name (bool, float, etc..).
   * @return Field Type Name.
   */
  public String getTypeName() {
    return type;
  }

  /**
   * Return the size occupied by this field in a GDAT Record.
   * @return Size in Bytes.
   */
  public int getSize() {
    return size;
  }

  /**
   * Return the {@link com.continuuity.internal.io.Schema} of this GDAT field
   * @return {@link com.continuuity.internal.io.Schema}
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Return the {@link co.cask.tigon.sql.api.GDATFieldType} given a stream engine field type name.
   * @param typeName Field Type Name used in Stream Engine.
   * @return {@link co.cask.tigon.sql.api.GDATFieldType}
   */
  public static GDATFieldType getGDATFieldType(String typeName) {
    for (GDATFieldType gdatFieldType : GDATFieldType.values()) {
      if (gdatFieldType.getTypeName().equals(typeName.toLowerCase())) {
        return gdatFieldType;
      }
    }
    return null;
  }
}
