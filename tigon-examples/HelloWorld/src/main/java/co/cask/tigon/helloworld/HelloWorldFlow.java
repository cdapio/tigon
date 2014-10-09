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

package co.cask.tigon.helloworld;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A HelloWorld {@link Flow}.
 * <p>HelloWorldFlow contains one flowlet that emits 'Hello World' and another flowlet that counts the number of times
 * it received 'HelloWorld' and logs it.</p>
 */
public class HelloWorldFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("HelloWorld")
      .setDescription("A flow that emits and logs 'Hello World' messages")
      .withFlowlets()
      .add("generator", new HelloWorldFlowlet(), 1)
      .add("logger", new LoggerFlowlet(), 1)
      .connect()
      .from("generator").to("logger")
      .build();
  }

  /**
   * Hello World Source Flowlet which emits 'Hello World' every second.
   */
  public static class HelloWorldFlowlet extends AbstractFlowlet {
    private OutputEmitter<String> output;

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void generate() throws InterruptedException {
      output.emit("Hello World");
    }
  }

  /**
   * Logger Flowlet prints the message it received and the number of messages received so far.
   */
  private static final class LoggerFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerFlowlet.class);
    private int count = 0;
    private int flowletId = 0;

    @Override
    public void initialize(FlowletContext context) {
      flowletId = context.getInstanceId();
    }

    @ProcessInput
    public void process(String input) {
      LOG.info("LoggerFlowlet{} Received: {} - {} time(s)", flowletId, input, ++count);
    }
  }

}
