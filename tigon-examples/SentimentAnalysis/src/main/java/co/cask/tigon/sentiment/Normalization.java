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
package co.cask.tigon.sentiment;


import co.cask.tigon.api.annotation.Batch;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes the sentences.
 */
public class Normalization extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

  private OutputEmitter<String> out;

  @ProcessInput
  @Batch(100)
  public void process(String text) {
    if (text != null) {
      LOG.info("Received tweet: " + text);
      out.emit(text);
    }
  }
}
