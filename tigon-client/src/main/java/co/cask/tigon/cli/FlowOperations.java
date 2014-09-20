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

package co.cask.tigon.cli;

import com.google.common.util.concurrent.Service;

import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Flow Operations.
 */
public interface FlowOperations extends Service {

  void deployFlow(File jarPath, String className);

  List<String> listAllFlows();

  void stopFlow(String flowName);

  List<InetSocketAddress> discover(String flowName, String service);

  void setInstances(String flowName, String flowInstance, int instanceCount);

  List<String> getFlowInfo(String flowName);

  void addLogHandler(String flowName, PrintStream out);
}
