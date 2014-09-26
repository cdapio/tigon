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

package co.cask.tigon.sql.io;

import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericTestClass
 * Sample flowlet used for testing the POJOCreator
 */

class GenericTestFlowlet<T, VV> extends AbstractInputFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(GenericTestFlowlet.class);

  @Override
  public void create() {}

  @QueryOutput("sumOut")
  boolean process0(T obj) {
    LOG.info("[POJOTestOutput][Process0] " + obj.toString());
    return true;
  }

  class Output {
    int timestamp;
    int iStream;

    public String toString() {
      return "\tTimeStamp: " + timestamp + "\tiStream: " + iStream;
    }
  }

  @QueryOutput("sumOut")
  boolean process1(Output obj) {
    LOG.info("[POJOTestOutput][Process1] " + obj.toString());
    return true;
  }

  @QueryOutput("sumOut")
  boolean process2(Output2 obj) {
    LOG.info("[POJOTestOutput][Process2] " + obj.toString());
    return true;
  }
}

/**
 * Sample output classes to test various use cases
 */

class Output1 {
  int timestamp;
  int iStream;
  String stringVar;

  public String toString() {
    return "\tTimeStamp: " + timestamp + "\tiStream: " + iStream + "\tString: " + stringVar;
  }
}

class Output2 {
  Long longVar;

  public String toString() {
    return "\tLongVar: " + longVar;
  }
}

class Output3 {
  Double badDoubleVar;

  public String toString() {
    return "\tdoubleVar: " + badDoubleVar;
  }
}

class Output4 {
  int boolVar;

  public String toString() {
    return "\tboolVar: " + boolVar;
  }
}

class GenericFlowletTestClass extends GenericTestFlowlet<Output1, Integer> {

}
