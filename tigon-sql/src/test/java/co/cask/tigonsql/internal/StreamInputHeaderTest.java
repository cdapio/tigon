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

package co.cask.tigonsql.internal;

import co.cask.tigonsql.api.GDATFieldType;
import co.cask.tigonsql.api.GDATSlidingWindowAttribute;
import co.cask.tigonsql.api.StreamSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the StreamInputHeader.
 */
public class StreamInputHeaderTest {
  private static final String schemaName = "purchaseStream";
  private static final StreamSchema testSchema = new StreamSchema.Builder()
    .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
    .addField("itemid", GDATFieldType.STRING)
    .addField("cost", GDATFieldType.INT, GDATSlidingWindowAttribute.NONE)
    .addField("homeowner", GDATFieldType.BOOL)
    .build();

  @Test
  public void testGDATHeaderGeneration() {
    StreamInputHeader inputHeader = new StreamInputHeader(schemaName, testSchema);
    String gdatHeader = inputHeader.getStreamHeader();
    int length = inputHeader.getStreamParseTree().length();
    Assert.assertTrue(gdatHeader.contains("SCHEMALENGTH:" + Integer.toString(length)));
  }
}
