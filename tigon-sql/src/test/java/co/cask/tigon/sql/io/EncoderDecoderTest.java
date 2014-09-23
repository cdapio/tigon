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

package co.cask.tigon.sql.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Test the GDATEncoder and GDATDecoder.
 */
public class EncoderDecoderTest {

  @Test
  public void testBasics() throws Exception {
    GDATEncoder encoder = new GDATEncoder();
    encoder.writeBool(true);
    encoder.writeBool(false);
    encoder.writeInt(Integer.MAX_VALUE);
    encoder.writeInt(Integer.MIN_VALUE);
    encoder.writeDouble(89723.2348734);
    encoder.writeDouble(-3.4);
    encoder.writeString("Hello");
    encoder.writeLong(Long.MAX_VALUE);
    encoder.writeLong(Long.MIN_VALUE);
    encoder.writeString("HelloHelloHelloHello");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    encoder.writeTo(outputStream);
    byte[] byteArray = outputStream.toByteArray();
    GDATDecoder decoder = new GDATDecoder(ByteBuffer.wrap(byteArray));
    //Subtract the 4 byte of length field which is not included in the data record length.
    Assert.assertEquals(byteArray.length - 4, decoder.getRecordLength());
    Assert.assertEquals(true, decoder.readBool());
    Assert.assertEquals(false, decoder.readBool());
    Assert.assertEquals(Integer.MAX_VALUE, decoder.readInt());
    Assert.assertEquals(Integer.MIN_VALUE, decoder.readInt());
    Assert.assertTrue(Double.compare(Double.valueOf("89723.2348734"), decoder.readDouble()) == 0);
    Assert.assertTrue(Double.compare(Double.valueOf("-3.4"), decoder.readDouble()) == 0);
    Assert.assertEquals("Hello", decoder.readString());
    Assert.assertEquals(Long.MAX_VALUE, decoder.readLong());
    Assert.assertEquals(Long.MIN_VALUE, decoder.readLong());
    Assert.assertEquals("HelloHelloHelloHello", decoder.readString());
  }

  @Test
  public void testEmptyString() throws Exception {
    GDATEncoder encoder = new GDATEncoder();
    encoder.writeInt(4);
    encoder.writeString("");
    encoder.writeLong(10);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    encoder.writeTo(outputStream);
    byte[] byteArray = outputStream.toByteArray();
    GDATDecoder decoder = new GDATDecoder(ByteBuffer.wrap(byteArray));
    Assert.assertEquals(4, decoder.readInt());
    Assert.assertEquals("", decoder.readString());
    Assert.assertEquals(10L, decoder.readLong());
  }
}
