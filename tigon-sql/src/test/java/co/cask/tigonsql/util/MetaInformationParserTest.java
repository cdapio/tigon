/**
 * Copyright 2012-2014 Cask, Inc.
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


package co.cask.tigonsql.util;

import co.cask.tigonsql.api.GDATField;
import co.cask.tigonsql.api.GDATFieldType;
import co.cask.tigonsql.api.GDATSlidingWindowAttribute;
import co.cask.tigonsql.api.StreamSchema;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

/**
 * MetaInformationParserTest
 */
public class MetaInformationParserTest {

  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testHFTACount() throws IOException {
    Location fileLocation = new LocalLocationFactory(tmp.newFolder()).create(tmp.newFolder().toURI());
    Writer writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(fileLocation.append("qtree.xml").toURI().getPath()), "utf-8"));
    writer.write("<?xml version='1.0' encoding='ISO-8859-1'?>" +
                   "<?xml-stylesheet type='text/xsl' href='qtree.xsl'?>" +
                   "<QueryNodes>" +
                   "<HFTA name='sum1'>" +
                   "<FileName value='hfta_0.cc' />" +
                   "<Rate value='100' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='s' pos='1' type='UINT'  />" +
                   "<LivenessTimeout value='5' />" +
                   "<ReadsFrom value='_fta_sum1' />" +
                   "<Ifaces_referenced value='[default]'/>" +
                   "</HFTA>" +
                   "<HFTA name='sum2'>" +
                   "<FileName value='hfta_0.cc' />" +
                   "<Rate value='100' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='DECREASING '  />" +
                   "<Field name='s1' pos='1' type='INT'  />" +
                   "<Field name='s2' pos='2' type='UINT'  />" +
                   "<Field name='s3' pos='3' type='UINT'  />" +
                   "<Field name='s4' pos='4' type='USHORT'  />" +
                   "<Field name='s5' pos='5' type='BOOL'  />" +
                   "<Field name='s6' pos='6' type='LLONG'  />" +
                   "<Field name='s7' pos='7' type='FLOAT'  />" +
                   "<Field name='s8' pos='8' type='VSTRING'  />" +
                   "<Field name='s9' pos='9' type='V_STR'  />" +
                   "<Field name='s10' pos='10' type='STRING'  />" +
                   "<LivenessTimeout value='5' />" +
                   "<ReadsFrom value='_fta_sum1' />" +
                   "<Ifaces_referenced value='[default]'/>" +
                   "</HFTA>" +
                   "<LFTA name='_fta_sum1' >" +
                   "<HtSize value='4096' />" +
                   "<Host value='MacBook-Pro.local' />" +
                   "<Interface  value='GDAT0' />" +
                   "<ReadsFrom value='GDAT0' />" +
                   "<Rate value='100' />" +
                   "<LivenessTimeout value='5' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='SUM_iStream' pos='1' type='UINT'  />" +
                   "</LFTA>" +
                   "<LFTA name='_fta_sum2' >" +
                   "<HtSize value='4096' />" +
                   "<Host value='Gokuls-MacBook-Pro.local' />" +
                   "<Interface  value='GDAT0' />" +
                   "<ReadsFrom value='GDAT0' />" +
                   "<Rate value='100' />" +
                   "<LivenessTimeout value='5' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='SUM_iStream' pos='1' type='UINT'  />" +
                   "</LFTA>" +
                   "</QueryNodes>");
    writer.close();
    int count;
    try {
      count = MetaInformationParser.getHFTACount(new File(fileLocation.toURI()));
    } catch (Exception e) {
      count = -1;
    }
    Assert.assertEquals(2, count);
  }

  @Test
  public void testSchemaMap() throws IOException {
    Location fileLocation = new LocalLocationFactory(tmp.newFolder()).create(tmp.newFolder().toURI());
    Writer writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(fileLocation.append("qtree.xml").toURI().getPath()), "utf-8"));
    //Creating sample qtree.xml
    writer.write("<?xml version='1.0' encoding='ISO-8859-1'?>" +
                   "<?xml-stylesheet type='text/xsl' href='qtree.xsl'?>" +
                   "<QueryNodes>" +
                   "<HFTA name='sum1'>" +
                   "<FileName value='hfta_0.cc' />" +
                   "<Rate value='100' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='s' pos='1' type='UINT'  />" +
                   "<LivenessTimeout value='5' />" +
                   "<ReadsFrom value='_fta_sum1' />" +
                   "<Ifaces_referenced value='[default]'/>" +
                   "</HFTA>" +
                   "<HFTA name='sum2'>" +
                   "<FileName value='hfta_0.cc' />" +
                   "<Rate value='100' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='DECREASING '  />" +
                   "<Field name='s1' pos='1' type='INT'  />" +
                   "<Field name='s2' pos='2' type='UINT'  />" +
                   "<Field name='s3' pos='3' type='UINT'  />" +
                   "<Field name='s4' pos='4' type='USHORT'  />" +
                   "<Field name='s5' pos='5' type='BOOL'  />" +
                   "<Field name='s6' pos='6' type='LLONG'  />" +
                   "<Field name='s7' pos='7' type='FLOAT'  />" +
                   "<Field name='s8' pos='8' type='VSTRING'  />" +
                   "<Field name='s9' pos='9' type='V_STR'  />" +
                   "<Field name='s10' pos='10' type='STRING'  />" +
                   "<LivenessTimeout value='5' />" +
                   "<ReadsFrom value='_fta_sum1' />" +
                   "<Ifaces_referenced value='[default]'/>" +
                   "</HFTA>" +
                   "<LFTA name='_fta_sum1' >" +
                   "<HtSize value='4096' />" +
                   "<Host value='MacBook-Pro.local' />" +
                   "<Interface  value='GDAT0' />" +
                   "<ReadsFrom value='GDAT0' />" +
                   "<Rate value='100' />" +
                   "<LivenessTimeout value='5' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='SUM_iStream' pos='1' type='UINT'  />" +
                   "</LFTA>" +
                   "<LFTA name='_fta_sum2' >" +
                   "<HtSize value='4096' />" +
                   "<Host value='MacBook-Pro.local' />" +
                   "<Interface  value='GDAT0' />" +
                   "<ReadsFrom value='GDAT0' />" +
                   "<Rate value='100' />" +
                   "<LivenessTimeout value='5' />" +
                   "<Field name='timestamp' pos='0' type='ULLONG' mods='INCREASING '  />" +
                   "<Field name='SUM_iStream' pos='1' type='UINT'  />" +
                   "</LFTA>" +
                   "</QueryNodes>");
    writer.close();
    Writer writer1 = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(fileLocation.append("output_spec.cfg").toURI().getPath()), "utf-8"));
    writer1.write("sum2,stream,,,,,\n" +
                    "sum1,stream,,,,,");
    writer1.close();
    Map<String, StreamSchema> schemaMap = MetaInformationParser.getSchemaMap(new File(fileLocation.toURI()));
    Assert.assertEquals(2, schemaMap.size());
    List<GDATField> schema1 = schemaMap.get("sum1").getFields();
    Assert.assertEquals(2, schema1.size());
    Assert.assertTrue(schema1.get(0).getName().equals("timestamp"));
    Assert.assertTrue(schema1.get(0).getType().equals(GDATFieldType.LONG));
    Assert.assertTrue(schema1.get(0).getSlidingWindowType().equals(GDATSlidingWindowAttribute.INCREASING));
    Assert.assertTrue(schema1.get(1).getName().equals("s"));
    Assert.assertTrue(schema1.get(1).getType().equals(GDATFieldType.INT));

    List<GDATField> schema2 = schemaMap.get("sum2").getFields();
    Assert.assertEquals(11, schema2.size());
    Assert.assertTrue(schema2.get(0).getName().equals("timestamp"));
    Assert.assertTrue(schema2.get(0).getType().equals(GDATFieldType.LONG));
    Assert.assertTrue(schema2.get(0).getSlidingWindowType().equals(GDATSlidingWindowAttribute.DECREASING));
    for (int i = 1; i < 11; i++) {
      Assert.assertTrue(schema2.get(i).getName().equals("s" + i));
      if (i < 6) {
        Assert.assertTrue(schema2.get(i).getType().equals(GDATFieldType.INT));
      } else if (i == 6) {
        Assert.assertTrue(schema2.get(i).getType().equals(GDATFieldType.LONG));
      } else if (i == 7) {
        Assert.assertTrue(schema2.get(i).getType().equals(GDATFieldType.DOUBLE));
      } else if ((i > 7) && (i < 11)) {
        Assert.assertTrue(schema2.get(i).getType().equals(GDATFieldType.STRING));
      }
    }
  }
}
