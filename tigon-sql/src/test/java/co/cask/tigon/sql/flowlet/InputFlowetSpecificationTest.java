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

package co.cask.tigon.sql.flowlet;

import co.cask.tigon.sql.internal.DefaultInputFlowletConfigurer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests the InputFlowletSpecification.
 */
public class InputFlowetSpecificationTest {

  @Test
  public void testBasicFlowlet() {
    AbstractInputFlowlet flowlet = new AllFieldTypeFlowlet();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    InputFlowletSpecification spec = configurer.createInputFlowletSpec();
    Assert.assertEquals(spec.getName(), "summation");
    Assert.assertEquals(spec.getDescription(), "sums up the input value over a timewindow");
    Assert.assertEquals(spec.getInputSchemas().size(), 1);
    Assert.assertTrue(spec.getInputSchemas().containsKey("intInput"));
    Assert.assertEquals(spec.getInputSchemas().size(), 1);
    Map.Entry<InputStreamFormat, StreamSchema> inputInfo = spec.getInputSchemas().get("intInput");
    Assert.assertEquals(inputInfo.getKey(), InputStreamFormat.JSON);
    Assert.assertEquals(inputInfo.getValue().getFields().size(), 5);
    Assert.assertEquals(spec.getQuery().size(), 1);
  }

  @Test
  public void testInvalidSchemaFlowlet() {
    AbstractInputFlowlet flowlet = new InvalidInputFlowlet();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    int testValue = 0;
    try {
      flowlet.create(configurer);
    } catch (Exception e) {
      testValue = 1;
    } finally {
      Assert.assertEquals(testValue, 1);
    }
  }
}


