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

package co.cask.tigonsql.io;

import com.continuuity.common.io.Encoder;
import co.cask.tigonsql.api.GDATRecordType;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

/**
 * A {@link Encoder} that performs all writes to an in memory buffer.
 */
public class GDATEncoder implements Encoder {
  private static final ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
  private final OutStream buffer;
  // Location, String
  private final List<Map.Entry<Integer, byte[]>> stringOffsetLocations = Lists.newArrayList();
  //Size of String Payload
  private int stringPayloadSize = 0;

  /** ByteBuffer objects shared by all functions in this object **/
  private final ByteBuffer sharedByteBuffer;

  /**
   * GDATEncoder Constructor
   */
  public GDATEncoder() {
    buffer = new OutStream();
    sharedByteBuffer = ByteBuffer.allocate(8).order(byteOrder);
  }

  /**
   * Customized child class of ByteArrayOutputStream that allows direct access to the underlying byte array.
   * [Created to avoid duplicating byte arrays]
   */
  private static final class OutStream extends ByteArrayOutputStream {

    /**
     * @return reference to the underlying byte array
     */
    public byte[] getByteArray() {
      return buf;
    }
  }

  /**
   * Write Length in Big Endian Order as per GDAT format specification.
   */
  private void writeLengthToStream(int length, OutputStream out) throws IOException {
    sharedByteBuffer.clear();
    sharedByteBuffer.order(ByteOrder.BIG_ENDIAN).putInt(length);
    sharedByteBuffer.flip();
    out.write(sharedByteBuffer.array(), 0, Ints.BYTES);
    sharedByteBuffer.order(byteOrder);
  }

  private void writeIntToStream(int val, OutputStream out) throws IOException {
    sharedByteBuffer.clear();
    sharedByteBuffer.putInt(val);
    sharedByteBuffer.flip();
    out.write(sharedByteBuffer.array(), 0, Ints.BYTES);
  }

  @Override
  public Encoder writeNull() throws IOException {
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    return writeInt(b ? 1 : 0);
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    //Get all four Bytes of Signed Integer.
    writeIntToStream(i, buffer);
    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    sharedByteBuffer.clear();
    sharedByteBuffer.putLong(l);
    sharedByteBuffer.flip();
    buffer.write(sharedByteBuffer.array(), 0, Longs.BYTES);
    return this;
  }

  @Override
  public Encoder writeFloat(float v) throws IOException {
    //GDAT format will only contain double data type values and will not contain any float type values
    throw new UnsupportedOperationException("Does not support Float type objects, use Double");
  }

  @Override
  public Encoder writeDouble(double v) throws IOException {
    //Get all 8 Bytes of a Double.
    sharedByteBuffer.clear();
    sharedByteBuffer.putDouble(v);
    sharedByteBuffer.flip();
    buffer.write(sharedByteBuffer.array(), 0, Doubles.BYTES);
    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    // String data to be inserted at location -> buffer.size()
    byte[] stringBytes = s.getBytes(Charsets.UTF_8);
    stringPayloadSize += stringBytes.length;
    stringOffsetLocations.add(Maps.immutableEntry(buffer.size(), stringBytes));
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("Does not support reading/writing Bytes");
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int i, int i2) throws IOException {
    throw new UnsupportedOperationException("Does not support reading/writing Bytes");
  }

  @Override
  public Encoder writeBytes(ByteBuffer byteBuffer) throws IOException {
    throw new UnsupportedOperationException("Does not support reading/writing Bytes");
  }

  private void writeToRecord(OutputStream outputStream, byte recordMarker) throws IOException {
    buffer.write(recordMarker);
    // Maintains the location of the next byte to be written to the outputStream
    int outputArrayCounter = 0;
    int offsetPointer = buffer.size() + stringOffsetLocations.size() * 3 * Ints.BYTES;
    byte[] bufferByteArray = buffer.getByteArray();

    //4 Bytes unsigned int to represent length of the data record.
    writeLengthToStream(offsetPointer + stringPayloadSize, outputStream);

    for (Map.Entry<Integer, byte[]> stringEntry : stringOffsetLocations) {
      // Bytes preceding location of string and after last outputArrayCounter
      outputStream.write(bufferByteArray, outputArrayCounter, stringEntry.getKey() - outputArrayCounter);
      byte[] stringBytes = stringEntry.getValue();
      // Write length of string
      writeIntToStream(stringBytes.length, outputStream);
      // Write offset of string
      writeIntToStream(offsetPointer, outputStream);
      // Write reserved
      writeIntToStream(0, outputStream);
      outputArrayCounter = stringEntry.getKey();
      offsetPointer += stringBytes.length;
    }
    // Write the remaining bytes from buffer
    outputStream.write(bufferByteArray, outputArrayCounter, buffer.size() - outputArrayCounter);
    // Write String values at the end of the record
    for (Map.Entry<Integer, byte[]> entry : stringOffsetLocations) {
      outputStream.write(entry.getValue());
    }
    buffer.reset();
  }

  /**
   * Writes a GDAT format DATA record to the provided outputStream. This method is to be called after calling all the
   * required write*() methods.
   *
   * @param outputStream GDAT record is written to this outputStream
   * @throws IOException
   */
  public void writeTo(OutputStream outputStream) throws IOException {
    writeToRecord(outputStream, GDATRecordType.DATA.getRecordMarker());
  }

  /**
   * Writes a GDAT format EOF record to the provided outputStream. This method is to be called after calling all the
   * required write*() methods.
   *
   * @param outputStream GDAT record is written to this outputStream
   * @throws IOException
   */
  public void writeEOFRecord(OutputStream outputStream) throws IOException {
    writeToRecord(outputStream, GDATRecordType.EOF.getRecordMarker());
  }
}
