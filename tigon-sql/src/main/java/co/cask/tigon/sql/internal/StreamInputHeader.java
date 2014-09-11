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

package co.cask.tigon.sql.internal;

import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.StreamSchema;
import com.google.common.annotations.VisibleForTesting;

/**
 * Generate Stream Headers for Input Streams.
 */
public class StreamInputHeader {
  //TODO: Get this from a config file?
  private static final String VERSION = "4";
  private final String name;
  private final StreamSchema schema;

  public StreamInputHeader(String name, StreamSchema schema) {
    this.name = name;
    this.schema = schema;
  }

  /**
   * Takes a name for the Schema and the StreamSchema object to create the contents for StreamTree.
   * Sample header :
   * FTA {
   *   STREAM inputStream {
   *     ullong timestamp get_gdat_ullong_pos1 (increasing);
   *     uint istream get_gdata_uint_pos2;
   *   }
   *   DEFINE {
   *     query_name 'example';
   *     _referenced_ifaces '[default]';
   *     visibility 'external';
   *   }
   *   PARAM { }
   *   SELECT foo FROM bar
   * }
   */
   @VisibleForTesting
   String getStreamParseTree() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("FTA {").append(Constants.NEWLINE).append("STREAM ").append(name).append(" {")
      .append(Constants.NEWLINE);
    stringBuilder.append(StreamSchemaCodec.serialize(schema));
    stringBuilder.append("}").append(Constants.NEWLINE)
      .append("DEFINE {").append(Constants.NEWLINE)
      .append(String.format("query_name '%s';", name)).append(Constants.NEWLINE)
      .append(String.format("_referenced_ifaces '[%s]';", "default")).append(Constants.NEWLINE)
      .append("visibility 'external';").append(Constants.NEWLINE).append("}").append(Constants.NEWLINE)
      .append("PARAM {").append(Constants.NEWLINE).append("}").append(Constants.NEWLINE)
      .append("SELECT foo FROM bar").append(Constants.NEWLINE).append("}").append(Constants.NEWLINE).append('\0');
    return stringBuilder.toString();
  }

  /**
   * "GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n"
   * SchemaLength : The size of StreamTree.
   */
  public String getStreamHeader() {
    StringBuilder builder = new StringBuilder();
    builder.append("GDAT").append(Constants.NEWLINE).append("VERSION:").append(VERSION).append(Constants.NEWLINE)
      .append("SCHEMALENGTH:");
    String streamTree = getStreamParseTree();
    int headerLength = streamTree.length();
    builder.append(Integer.toString(headerLength));
    builder.append(Constants.NEWLINE);
    builder.append(streamTree);
    return builder.toString();
  }
}
