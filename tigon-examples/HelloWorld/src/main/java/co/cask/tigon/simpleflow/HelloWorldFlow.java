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

package co.cask.tigon.simpleflow;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A Hello World Flow {@link Flow}.
 */
public class HelloWorldFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("HelloWorld")
      .setDescription("HelloWorld Flow")
      .withFlowlets()
      .add("generator", new HelloWorldFlowlet())
      .add("logger", new LoggerFlowlet())
      .connect()
      .from("generator").to("logger")
      .build();
  }

  /**
   * Hello World Source Flowlet.
   */
  public static class HelloWorldFlowlet extends AbstractFlowlet {
    private OutputEmitter<String> output;

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void generate() throws InterruptedException {
      output.emit("Hello World");
    }
  }

  /**
   * Logger Flowlet.
   */
  private static final class LoggerFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerFlowlet.class);

    @ProcessInput
    public void process(String input) {
      LOG.info("Received: {}", input);
    }
  }

}
