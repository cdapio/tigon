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

package co.cask.tigon.sqljoinflow;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.Flowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.GDATFieldType;
import co.cask.tigon.sql.flowlet.GDATSlidingWindowAttribute;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Flow} that does an inner-join on age and name, based on ID.
 */
public class SQLJoinFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("SQLFlow")
      .setDescription("Testing SQLFlowlet")
      .withFlowlets()
      .add("SQLInput", new SQLInputFlowlet(), 1)
      .add("Digest", new DigestFlowlet(), 4)
      .connect()
      .from("SQLInput").to("Digest")
      .build();
  }

  /**
   * A {@link AbstractInputFlowlet} that does an inner join on age and name, based on ID.
   * It then emits the event to the Digest Flowlet.
   */
  public static class SQLInputFlowlet extends AbstractInputFlowlet {

    private OutputEmitter<OutObj> emitter;
    private static final Logger LOG = LoggerFactory.getLogger(SQLInputFlowlet.class);

    @Override
    public void create() {
      setName("SQLinputFlowlet");
      setDescription("Executes an inner join of two streams <uid, name>, <uid, age> and emits <uid, name, age>");
      StreamSchema nameSchema = new StreamSchema.Builder()
        .setName("nameDataStream")
        .addField("uid", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
        .addField("name", GDATFieldType.STRING)
        .build();
      addJSONInput("nameInput", nameSchema);

      StreamSchema ageSchema = new StreamSchema.Builder()
        .setName("ageDataStream")
        .addField("uid", GDATFieldType.INT, GDATSlidingWindowAttribute.INCREASING)
        .addField("age", GDATFieldType.INT)
        .build();
      addJSONInput("ageInput", ageSchema);

      addQuery("userDetails", "SELECT nI.uid, nI.name, aI.age INNER_JOIN " +
        "FROM nameInput.nameDataStream nI, ageInput.ageDataStream aI WHERE nI.uid = aI.uid AND aI.age > 40");
    }

    @QueryOutput("userDetails")
    void process(OutObj obj) throws Exception {
      LOG.info("Sending Event : UID = {}; Name = {}; Age = {}", obj.uid, obj.name, obj.age);
      emitter.emit(obj, "hash", obj.hashCode());
    }
  }

  /**
   * A {@link Flowlet} that receives data and logs it.
   */
  public static class DigestFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(DigestFlowlet.class);

    @ProcessInput
    void process(OutObj obj) throws Exception {
      LOG.info("Received Event : UID = {}; Name = {}; Age = {}", obj.uid, obj.name, obj.age);
    }
  }

  private class OutObj {
    int uid;
    String name;
    int age;
  }
}
