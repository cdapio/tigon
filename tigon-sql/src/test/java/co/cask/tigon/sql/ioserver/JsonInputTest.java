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

package co.cask.tigon.sql.ioserver;

import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATRecordQueue;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.internal.StreamEngineSimulator;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test Json Input Format Conversion.
 */
public class JsonInputTest {
  private static final Logger LOG = LoggerFactory.getLogger(ServerSocketServiceTest.class);
  private static final String name = "IntegerStream";
  private static final StreamSchema schema = new StreamSchema.Builder()
    .addField("timestamp", GDATFieldType.INT)
    .addField("intStream", GDATFieldType.INT)
    .addField("randomStr", GDATFieldType.STRING)
    .build();

  private static final ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                                  Executors.newCachedThreadPool());
  @Test
  public void basicJsonSocketTest() throws Exception {
    InputServerSocket inputSocketService = new JsonInputServerSocket(factory, name, schema);
    OutputServerSocket outputSocketService = new OutputServerSocket(factory, name, null, new GDATRecordQueue());
    StreamEngineSimulator simulator = null;
    try {
      inputSocketService.startAndWait();
      outputSocketService.startAndWait();
      simulator = new StreamEngineSimulator(
        inputSocketService.getSocketAddressMap().get(Constants.StreamIO.DATASOURCE),
        outputSocketService.getSocketAddressMap().get(Constants.StreamIO.DATASINK),
        inputSocketService.getSocketAddressMap().get(Constants.StreamIO.TCP_DATA_INGESTION));
      LOG.info(inputSocketService.getSocketAddressMap().toString());
      LOG.info(outputSocketService.getSocketAddressMap().toString());
      simulator.startAndWait();

      //Wait for connections to be setup
      TimeUnit.SECONDS.sleep(1);

      String jsonData = "{ \"data\" : [\"1234\", \"5\", \"Hello\"]}";
      byte[] jsonBytes = jsonData.getBytes();
      simulator.sendData(jsonBytes);
      simulator.sendData(jsonBytes);
      simulator.sendData(jsonBytes);
      simulator.sendData(jsonBytes);

      //Wait for data to be sent across
      TimeUnit.SECONDS.sleep(1);
      Assert.assertEquals(outputSocketService.getDataRecordsReceived(), 4);
    } finally {
      inputSocketService.stopAndWait();
      if (simulator != null) {
        simulator.stopAndWait();
      }
      outputSocketService.stopAndWait();
    }
  }
}
