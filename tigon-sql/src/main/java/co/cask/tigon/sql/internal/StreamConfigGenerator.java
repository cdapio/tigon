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

import co.cask.tigon.sql.api.InputStreamFormat;
import co.cask.tigon.sql.api.StreamSchema;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.InputFlowletSpecification;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Config File Content Generator.
 */
public class StreamConfigGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamConfigGenerator.class);
  private static final String OUTPUTSPEC_SUFFIX = ",stream,,,,,";
  private static final String HOSTNAME = "localhost";
  private static final String IFRESXML_CONTENT =
    "<Interface Name='%s'>" + Constants.NEWLINE +
    "<InterfaceType value='GDAT'/>" + Constants.NEWLINE +
    "<Filename value='%s'/>" + Constants.NEWLINE +
    "<Gshub value='1'/>" + Constants.NEWLINE +
    "<Verbose value='TRUE'/>" + Constants.NEWLINE +
    "</Interface>" + Constants.NEWLINE;
  private final InputFlowletSpecification spec;

  public StreamConfigGenerator(InputFlowletSpecification spec) {
    this.spec = spec;
  }

  public String generatePacketSchema() {
    return createPacketSchema(spec.getInputSchemas());
  }

  public String generateOutputSpec() {
    return createOutputSpec(spec.getQuery());
  }

  public Map<String, String> generateQueryFiles() {
    return createQueryFiles(spec.getQuery());
  }

  public Map.Entry<String, String> generateHostIfq() {
    String contents = createLocalHostIfq(HOSTNAME);
    return Maps.immutableEntry(HOSTNAME, contents);
  }

  public String generateIfresXML() {
    return createIfresXML(spec.getInputSchemas().keySet());
  }

  private String createIfresXML(Set<String> inputNames) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("<Resources>").append(Constants.NEWLINE).append("<Host Name='").append(HOSTNAME).append("'>")
      .append(Constants.NEWLINE);
    for (String name : inputNames) {
      stringBuilder.append(String.format(IFRESXML_CONTENT, name, name));
    }
    stringBuilder.append("</Host>").append(Constants.NEWLINE).append("</Resources>").append(Constants.NEWLINE);
    return stringBuilder.toString();
  }

  private String createLocalHostIfq(String hostname) {
    return "default : Contains[InterfaceType, GDAT]";
  }

  private String createOutputSpec(Map<String, String> gsql) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String outputStreamName : gsql.keySet()) {
      stringBuilder.append(outputStreamName).append(OUTPUTSPEC_SUFFIX).append(Constants.NEWLINE);
    }
    return stringBuilder.toString();
  }

  private String createPacketSchema(Map<String, Map.Entry<InputStreamFormat, StreamSchema>> schemaMap) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String streamName : schemaMap.keySet()) {
      stringBuilder.append(createProtocol(streamName, schemaMap.get(streamName).getValue()));
    }
    return stringBuilder.toString();
  }

  /**
   * Takes a name for the Schema and the StreamSchema object to create the contents for packet_schema.txt file.
   * Sample file content :
   * PROTOCOL name {
   *   ullong timestamp get_gdat_ullong_pos1 (increasing);
   *   uint istream get_gdat_uint_pos2;
   * }
   */
  private String createProtocol(String name, StreamSchema schema) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("PROTOCOL ").append(name).append(" {").append(Constants.NEWLINE);
    stringBuilder.append(StreamSchemaCodec.serialize(schema));
    stringBuilder.append("}").append(Constants.NEWLINE);
    return stringBuilder.toString();
  }

  private String createQueryContent(String name, String gsql) {
    StringBuilder stringBuilder = new StringBuilder();
    //TODO: Providing external visibility to all queries for now. Need to set this only for queries whose outputs
    //have corresponding process methods for better performance.
    String header = String.format("DEFINE { query_name '%s'; visibility 'external'; }", name);
    //Use default interface set
    stringBuilder.append(header).append(Constants.NEWLINE).append(gsql).append(Constants.NEWLINE);
    return stringBuilder.toString();
  }

  private Map<String, String> createQueryFiles(Map<String, String> gsql) {
    Map<String, String> gsqlQueryMap = Maps.newHashMap();
    for (Map.Entry<String, String> sqlQuery : gsql.entrySet()) {
      gsqlQueryMap.put(sqlQuery.getKey(), createQueryContent(sqlQuery.getKey(), sqlQuery.getValue()));
    }
    return gsqlQueryMap;
  }
}
