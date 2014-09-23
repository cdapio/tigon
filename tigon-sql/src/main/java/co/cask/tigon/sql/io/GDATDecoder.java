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

import co.cask.tigon.io.Decoder;
import com.google.common.base.Charsets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decode output stream data record.
 */
public class GDATDecoder implements Decoder {
  private final ByteBuffer dataRecord;
  private int recordLength;

  /**
   * Constructor for GDATDecoder
   * @param dataRecord incoming byte buffer object that represents the data to be decoded
   */
  public GDATDecoder(ByteBuffer dataRecord) {
    //Get the length of record which is encoded in Big Endian Byte Order.
    recordLength = dataRecord.order(ByteOrder.BIG_ENDIAN).getInt();
    this.dataRecord = dataRecord.slice().order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Resets the underlying ByteBuffer's position to 0
   */
  public void reset() {
    dataRecord.position(0);
  }

  /**
   * Get the length of the data record.
   */
  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public Object readNull() throws IOException {
    return null;
  }

  @Override
  public boolean readBool() throws IOException {
    return readInt() != 0;
  }

  @Override
  public int readInt() throws IOException {
    return dataRecord.getInt();
  }

  @Override
  public long readLong() throws IOException {
    return dataRecord.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    //GDAT format will only contain double data type values and will not contain any float type values
    throw new UnsupportedOperationException("Does not support Float type objects, use Double");
  }

  @Override
  public double readDouble() throws IOException {
    return dataRecord.getDouble();
  }

  @Override
  public String readString() throws IOException {
    int length = dataRecord.getInt();
    //Read Index (4 bytes) and skip Reserved field (4 bytes).
    int index = dataRecord.getInt();
    //Skipping through reserved integer field
    dataRecord.position(dataRecord.position() + Ints.BYTES);

    //4 Bytes occupied by length of record which is not included in index. So add 4.
    int oldLimit = dataRecord.limit();
    dataRecord.mark();
    dataRecord.position(index);
    dataRecord.limit(index + length);
    String str = Charsets.UTF_8.decode(dataRecord).toString();
    dataRecord.reset();
    dataRecord.limit(oldLimit);
    return str;
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    throw new UnsupportedOperationException("Does not support reading/writing Bytes");
  }

  @Override
  public void skipFloat() throws IOException {
    throw new UnsupportedOperationException("Does not support Float type objects, use Double");
  }

  @Override
  public void skipDouble() throws IOException {
    dataRecord.position(dataRecord.position() + Doubles.BYTES);
  }

  @Override
  public void skipString() throws IOException {
    //String is represented by 3 integers. The actual string data is added as payload at the end of record
    dataRecord.position(dataRecord.position() + 3 * Ints.BYTES);
  }

  @Override
  public void skipBytes() throws IOException {
    throw new UnsupportedOperationException("Does not support reading/writing Bytes");
  }
}
