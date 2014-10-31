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

package co.cask.tigon.conf;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {
  /**
   * HDFS Namespaces used in Distributed Mode.
   */
  public static final class Location {
    public static final String ROOT_NAMESPACE = "root.namespace";
    public static final String FLOWJAR = "flowjars";
  }

  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String TRANSACTION = "transaction";
  }

  /**
   * Transaction Configration.
   */
  public static final class Transaction {
    public static final String ADDRESS = "data.tx.bind.address";
  }

  /**
   * Zookeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String CFG_SESSION_TIMEOUT_MILLIS = "zookeeper.session.timeout.millis";
    public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 40000;
  }

  /**
   * Container Configuration.
   */
  public static final class Container {
    /**
     * JVM Options.
     */
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";
  }

  /**
   * Datasets.
   */
  public static final class Dataset {
    public static final String TABLE_PREFIX = "dataset.table.prefix";
  }

  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_TWILL_ZK_NAMESPACE = "twill.zookeeper.namespace";
  public static final String CFG_TWILL_RESERVED_MEMORY_MB = "twill.java.reserved.memory.mb";
  public static final String CFG_TWILL_NO_CONTAINER_TIMEOUT = "twill.no.container.timeout";

  /**
   * Config for Log Collection.
   */
  public static final String CFG_LOG_COLLECTION_ROOT = "log.collection.root";
  public static final String DEFAULT_LOG_COLLECTION_ROOT = "data/logs";
  public static final String CFG_LOG_COLLECTION_PORT = "log.collection.bind.port";
  public static final int DEFAULT_LOG_COLLECTION_PORT = 12157;
  public static final String CFG_LOG_COLLECTION_THREADS = "log.collection.threads";
  public static final int DEFAULT_LOG_COLLECTION_THREADS = 10;
  public static final String CFG_LOG_COLLECTION_SERVER_ADDRESS = "log.collection.bind.address";
  public static final String DEFAULT_LOG_COLLECTION_SERVER_ADDRESS = "localhost";


  public static final String DELTA_WRITE = "d";
}
