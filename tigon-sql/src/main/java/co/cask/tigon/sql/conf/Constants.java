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

package co.cask.tigon.sql.conf;

/**
 * Constants used by different parts of the system are all defined here.
 */
public final class Constants {
  /**
   * Stream I/O Server constants.
   */
  public static final class StreamIO {
    public static final String DATASOURCE = "datasource";
    public static final String DATASINK = "datasink";
    public static final String TCP_DATA_INGESTION = "tcpingestion";
    public static final String HTTP_DATA_INGESTION = "httpingestion";
  }

  public static final String STREAMENGINE_INSTANCE = "streamengine";
  public static final String NEWLINE = System.getProperty("line.separator");

  /**
  Value used for AbstractInputFlowlet's tick method timeout in Seconds
   */
  public static final int TICKER_TIMEOUT = 10;

  /**
   * The SQL Compiler process ping frequency is 1 ping per second. Using value larger than 1 to avoid boundary failure
   */
  public static final long HEARTBEAT_FREQUENCY = 5L;

  /**
   * The SQL Compiler processes require some setup time in the beginning before the ping the
   * {@link co.cask.tigon.sql.manager.DiscoveryServer} REST end-points to announce themselves
   */
  public static final long INITIALIZATION_TIMEOUT = 15L;

  /**
   * Maximum number of retries for each transaction by the flowlet
   */
  public static final int MAX_RETRY = 5;

  /**
   * File names of files parsed by {@link co.cask.tigon.sql.util.MetaInformationParser}
   */
  public static final String QTREE = "qtree.xml";
  public static final String OUTPUT_SPEC = "output_spec.cfg";
  public static final String GSQL_FILE = "tigon.gsql";

  /**
   * Data Ingestion Ports Map Keys
   */
  public static final String HTTP_PORT = "httpPort";
  public static final String TCP_INGESTION_PORT_PREFIX = "tcpPort_";
}
