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

package co.cask.tigon.sql.flowlet;

/**
 * Input Flowlet with two Streams.
 */
public class InputFlowletWithTwoStreams extends AbstractInputFlowlet {

  @Override
  public void create() {
    setName("summation");
    setDescription("sums up the input value over a timewindow from two streams");
    StreamSchema schema2 = new StreamSchema.Builder()
      .addField("timestamp", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
      .addField("stream", GDATFieldType.DOUBLE)
      .build();

    StreamSchema schema1 = new StreamSchema.Builder()
      .addField("timestamp", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
      .addField("stream", GDATFieldType.DOUBLE)
      .build();

    addGDATInput("intInput1", schema1);
    addGDATInput("intInput2", schema2);

    addQuery("sum1", "SELECT timestamp, SUM(stream) as s FROM intInput1 GROUP BY timestamp");
    addQuery("sum2", "SELECT timestamp, SUM(stream) as s FROM intInput2 GROUP BY timestamp");

    addQuery("sumOut",
            "SELECT s1.timestamp, s1.s, s2.s INNER_JOIN FROM sum1 s1, sum2 s2 WHERE s1.timestamp = s2.timestamp");
  }
}

