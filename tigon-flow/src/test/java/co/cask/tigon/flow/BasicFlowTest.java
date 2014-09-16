/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.tigon.flow;

import co.cask.tigon.api.annotation.HashPartition;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.api.metrics.Metrics;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BasicFlowTest extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BasicFlowTest.class);

  @Test
  public void testFlow() throws Exception {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    String tmpDir = tmpFolder.newFolder().toString();
    runtimeArgs.put("folder", tmpDir);
    LOG.info("Check {} dir for the files", tmpDir);

    FlowManager manager = deployFlow(BasicFlow.class, runtimeArgs);
    TimeUnit.SECONDS.sleep(5);
    manager.setFlowletInstances("generator", 4);
    TimeUnit.SECONDS.sleep(5);
    manager.setFlowletInstances("sink", 3);
    TimeUnit.SECONDS.sleep(5);
    manager.setFlowletInstances("generator", 2);
    manager.setFlowletInstances("sink", 1);
    TimeUnit.SECONDS.sleep(5);
    manager.stop();

  }
}

class BasicFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("testFlow")
      .setDescription("")
      .withFlowlets()
      .add("generator", new GeneratorFlowlet(), 2)
      .add("sink", new SinkFlowlet(), 2)
      .connect()
      .from("generator").to("sink")
      .build();
  }
}

class GeneratorFlowlet extends AbstractFlowlet {

  private FileWriter fileWriter;
  private OutputEmitter<Integer> intEmitter;
  private int i = 0;
  private static final Logger LOG = LoggerFactory.getLogger(GeneratorFlowlet.class);
  private Metrics metrics;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    String tmpFolder = context.getRuntimeArguments().get("folder");
    fileWriter = new FileWriter(new File(tmpFolder + "/flgen" + context.getInstanceId()));
    LOG.info("Getting Started");
  }

  @Tick(delay = 1L, unit = TimeUnit.SECONDS)
  public void process() throws Exception {
    Integer value = ++i;
    intEmitter.emit(value, "integer", value.hashCode());
    fileWriter.write(String.valueOf(value));
    fileWriter.write("\n");
    fileWriter.flush();
    LOG.info("Sending some data {}", value);
    metrics.count("sendData", 1);
  }

  @Override
  public void destroy() {
    try {
      fileWriter.close();
    } catch (Exception e) {

    }
  }
}

class SinkFlowlet extends AbstractFlowlet {
  private FileWriter fileWriter;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    String tmpFolder = context.getRuntimeArguments().get("folder");
    fileWriter = new FileWriter(new File(tmpFolder + "/flsink" + context.getInstanceId()));
  }

  @HashPartition("integer")
  @ProcessInput
  public void process(Integer value) throws Exception {
    fileWriter.write(String.valueOf(value));
    fileWriter.write("\n");
    fileWriter.flush();
  }

  @Override
  public void destroy() {
    try {
      fileWriter.close();
    } catch (Exception e) {

    }
  }
}
