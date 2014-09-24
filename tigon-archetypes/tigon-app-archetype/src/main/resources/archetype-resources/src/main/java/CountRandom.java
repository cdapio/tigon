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

package $package;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Count Random numbers {@link Flow}.
 */
public class CountRandom implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountRandom")
      .setDescription("CountRandom Flow")
      .withFlowlets()
      .add("source", new RandomSource())
      .add("counter", new NumberCounter())
      .connect()
      .from("source").to("counter")
      .build();
  }

  /**
   * Random Source Flowlet.
   */
  public class RandomSource extends AbstractFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }

  /**
   * Number Counter Flowlet.
   */
  private static final class NumberCounter extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(NumberCounter.class);

    @ProcessInput
    public void process(Integer number) {
      LOG.info("Received: " + number);
    }
  }

}
