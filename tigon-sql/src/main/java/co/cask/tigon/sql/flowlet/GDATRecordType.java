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

package co.cask.tigon.sql.flowlet;

/**
 * GDAT Record Type.
 */
public enum GDATRecordType {
  DATA((byte) 0),
  TRACE((byte) 1),
  EOF((byte) 2);

  private byte recordMarker;

  //Record Type.
  private GDATRecordType(byte recordMarker) {
    this.recordMarker = recordMarker;
  }

  /**
   * Return the Record Marker Byte value for the {@link GDATRecordType}.
   * @return Record Marker Byte.
   */
  public byte getRecordMarker() {
    return recordMarker;
  }

  /**
   * Return the {@link GDATRecordType} given a Record Marker Byte.
   * @param recordMarker Record Marker Byte
   * @return {@link GDATRecordType}
   */
  public static GDATRecordType getGDATRecordType(byte recordMarker) {
    for (GDATRecordType gdatRecordType : GDATRecordType.values()) {
      if (gdatRecordType.getRecordMarker() == recordMarker) {
        return gdatRecordType;
      }
    }
    return null;
  }
}
