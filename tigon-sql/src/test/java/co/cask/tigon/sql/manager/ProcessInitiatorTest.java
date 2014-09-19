/**
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.tigon.sql.manager;

import co.cask.tigon.sql.util.MetaInformationParser;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * ProcessInitiatorTest
 */

public class ProcessInitiatorTest {
  private static ProcessInitiator processInitiator;
  private static HubDataStore hubDataStore;

  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    List<HubDataSource> sourceList = Lists.newArrayList();
    sourceList.add(new HubDataSource("source1", new InetSocketAddress("127.0.0.1", 8081)));
    sourceList.add(new HubDataSource("source2", new InetSocketAddress("127.0.0.1", 8082)));
    List<HubDataSink> sinkList = Lists.newArrayList();
    sinkList.add(new HubDataSink("sink1", "query1", new InetSocketAddress("127.0.0.1", 7081)));
    sinkList.add(new HubDataSink("sink2", "query2", new InetSocketAddress("127.0.0.1", 7082)));
    hubDataStore = new HubDataStore.Builder()
      .setInstanceName("test")
      .setDataSource(sourceList)
      .setDataSink(sinkList)
      .setHFTACount(5)
      .setHubAddress(new InetSocketAddress("127.0.0.1", 2222))
      .setClearingHouseAddress(new InetSocketAddress("127.0.0.1", 1111))
      .setBinaryLocation(new LocalLocationFactory(tmp.newFolder()).create(tmp.newFolder().toURI()))
      .build();
    processInitiator = new ProcessInitiator(hubDataStore);
  }

  @Test
  public void testRTSExecution() throws IOException {
    File file = new File(hubDataStore.getBinaryLocation().append("rts").toURI().getPath());
    Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
    writer.write("echo $1 & $2");
    file.setExecutable(true, false);
    writer.close();
    processInitiator.startRTS();
  }

  @Test
  public void testHFTAExecution() throws IOException {
    Writer writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(hubDataStore.getBinaryLocation().append("qtree.xml").toURI().getPath()), "utf-8"));
    //Creating sample qtree.xml
    writer.write("<?xml version='1.0' encoding='ISO-8859-1'?>\n" +
                   "<?xml-stylesheet type='text/xsl' href='qtree.xsl'?>\n" +
                   "<QueryNodes>\n" +
                   "\t<HFTA name='sum1'>\n" +
                   "\t\t<FileName value='hfta_0.cc' />\n" +
                   "\t\t<Rate value='100' />\n" +
                   "\t\t<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />\n" +
                   "\t\t<Field name='s' pos='1' type='UINT'  />\n" +
                   "\t\t<LivenessTimeout value='5' />\n" +
                   "\t\t<ReadsFrom value='_fta_sum1' />\n" +
                   "\t\t<Ifaces_referenced value='[default]'/>\n" +
                   "\t</HFTA>\n" +
                   "\t<LFTA name='_fta_sum1' >\n" +
                   "\t\t<HtSize value='4096' />\n" +
                   "\t\t<Host value='MacBook-Pro.local' />\n" +
                   "\t\t<Interface  value='GDAT0' />\n" +
                   "\t\t<ReadsFrom value='GDAT0' />\n" +
                   "\t\t<Rate value='100' />\n" +
                   "\t\t<LivenessTimeout value='5' />\n" +
                   "\t\t<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />\n" +
                   "\t\t<Field name='SUM_iStream' pos='1' type='UINT'  />\n" +
                   "\t</LFTA>\n" +
                   "\t<LFTA name='_fta_sum2' >\n" +
                   "\t\t<HtSize value='4096' />\n" +
                   "\t\t<Host value='MacBook-Pro.local' />\n" +
                   "\t\t<Interface  value='GDAT0' />\n" +
                   "\t\t<ReadsFrom value='GDAT0' />\n" +
                   "\t\t<Rate value='100' />\n" +
                   "\t\t<LivenessTimeout value='5' />\n" +
                   "\t\t<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />\n" +
                   "\t\t<Field name='SUM_iStream' pos='1' type='UINT'  />\n" +
                   "\t</LFTA>\n" +
                   "</QueryNodes>\n");
    writer.close();
    int count;
    try {
      count =  MetaInformationParser.getHFTACount(new File(hubDataStore.getBinaryLocation().toURI()));
    } catch (Exception e) {
      count = -1;
    }
    Assert.assertEquals(1, count);

    File file = new File(hubDataStore.getBinaryLocation().append("hfta_0").toURI().getPath());
    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
    writer.write("echo $1 & $2");
    file.setExecutable(true, false);
    writer.close();
    processInitiator.startHFTA();
  }

  @Test
  public void testGSEXITExecution() throws IOException {
    File file = new File(hubDataStore.getBinaryLocation().append("GSEXIT").toURI().getPath());
    Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
    writer.write("echo $1 & $2");
    file.setExecutable(true, false);
    writer.close();
    processInitiator.startGSEXIT();
  }

}
