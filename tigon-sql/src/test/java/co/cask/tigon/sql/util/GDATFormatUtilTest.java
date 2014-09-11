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

package co.cask.tigon.sql.util;

import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATRecordType;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.internal.StreamInputHeader;
import co.cask.tigon.sql.internal.StreamSchemaCodec;
import co.cask.tigon.sql.io.GDATDecoder;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Test GDAT Format encoder/decoder Utility class.
 */
public class GDATFormatUtilTest {
  private static final String schemaName = "purchaseStream";
  private static final StreamSchema testSchema = new StreamSchema.Builder()
    .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
    .addField("itemid", GDATFieldType.STRING)
    .addField("cost", GDATFieldType.INT, GDATSlidingWindowAttribute.NONE)
    .addField("location", GDATFieldType.STRING)
    .build();

  @Test
  public void testGDATHeaderToSchema() {
    StreamInputHeader inputHeader = new StreamInputHeader(schemaName, testSchema);
    String header = inputHeader.getStreamHeader();
    String schemaString = GDATFormatUtil.getSerializedSchema(header);
    StreamSchema schema = StreamSchemaCodec.deserialize(schemaString);
    Assert.assertEquals(schema.getFields().size(), 4);
    Assert.assertEquals(schema.getFields().get(0).getName(), "timestamp");
    Assert.assertEquals(schema.getFields().get(1).getName(), "itemid");
    Assert.assertEquals(schema.getFields().get(2).getName(), "cost");
    Assert.assertEquals(schema.getFields().get(3).getName(), "location");
    Assert.assertEquals(schema.getFields().get(1).getType(), GDATFieldType.STRING);
    Assert.assertEquals(schema.getFields().get(2).getType(), GDATFieldType.INT);
    Assert.assertEquals(schema.getFields().get(3).getType(), GDATFieldType.STRING);
    //This parser ignores the SlidingWindowAttribute since it is not required for POJO creation.
    Assert.assertEquals(schema.getFields().get(0).getSlidingWindowType(), GDATSlidingWindowAttribute.NONE);
    Assert.assertEquals(schema.getFields().get(1).getSlidingWindowType(), GDATSlidingWindowAttribute.NONE);
    Assert.assertEquals(schema.getFields().get(2).getSlidingWindowType(), GDATSlidingWindowAttribute.NONE);
    Assert.assertEquals(schema.getFields().get(3).getSlidingWindowType(), GDATSlidingWindowAttribute.NONE);
  }

  @Test
  public void testEOFRecordGeneration() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GDATFormatUtil.encodeEOF(testSchema, out);
    byte[] eofRecordBytes = out.toByteArray();
    int fieldSize = GDATFormatUtil.getGDATFieldSize(testSchema);
    Assert.assertEquals(GDATRecordType.EOF, GDATRecordType.getGDATRecordType(eofRecordBytes[fieldSize + Ints.BYTES]));
    GDATDecoder decoder = new GDATDecoder(ByteBuffer.wrap(eofRecordBytes));
    Assert.assertEquals(0L, decoder.readLong());
    Assert.assertEquals("", decoder.readString());
    Assert.assertEquals(0, decoder.readInt());
    Assert.assertEquals("", decoder.readString());
  }

  @Test
  public void testFieldSize() throws IOException {
    Assert.assertEquals(36, GDATFormatUtil.getGDATFieldSize(testSchema));
  }
}
