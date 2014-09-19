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

package co.cask.tigon.sql.io;

import co.cask.tigon.internal.io.Schema;
import co.cask.tigon.internal.io.UnsupportedTypeException;
import co.cask.tigon.internal.lang.MethodVisitor;
import co.cask.tigon.internal.lang.Reflections;
import co.cask.tigon.sql.flowlet.AbstractInputFlowlet;
import co.cask.tigon.sql.flowlet.GDATField;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.flowlet.annotation.QueryOutput;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * MethodsDriver
 * This class is responsible for managing all the methods in the flowlet. It creates an instance of class
 * {@link co.cask.tigon.sql.io.MethodInvoker} for each method  annotated by
 * {@link co.cask.tigon.sql.flowlet.annotation.QueryOutput} in this flowlet class
 *
 */
public class MethodsDriver {
  private final Multimap<String, MethodInvoker> methodListMap;
  private final Map<String, StreamSchema> schemaMap;
  private final AbstractInputFlowlet flowlet;

  /**
   * Constructor for MethodsDriver
   * @param flowlet An instance of the {@link co.cask.tigon.sql.flowlet.AbstractInputFlowlet} to be used for
   *                invoking the method calls
   */
  public MethodsDriver(AbstractInputFlowlet flowlet, Map<String, StreamSchema> schemaMap) {
    this.methodListMap = HashMultimap.create();
    this.flowlet = flowlet;
    this.schemaMap = schemaMap;
    populateMethodListMap();
  }

  private void populateMethodListMap() {
    //Populate methodListMap
    Reflections.visit(flowlet, TypeToken.of(flowlet.getClass()),
                      new MethodVisitor() {
                        @Override
                        public void visit(Object o, TypeToken<?> inspectType, TypeToken<?> declareType, Method method)
                          throws Exception {
                          QueryOutput annotation = method.getAnnotation(QueryOutput.class);
                          if ((annotation == null) || (annotation.value().isEmpty())) {
                            return;
                          }
                          try {
                            methodListMap.put(annotation.value(),
                                              new MethodInvoker(o, method, inspectType
                                                , getSchema(schemaMap.get(annotation.value())))
                            );
                          } catch (UnsupportedTypeException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      }
    );
  }

  public Schema getSchema(StreamSchema streamSchema) {
    List<Schema.Field> fieldList = Lists.newArrayList();
    for (GDATField gdatField : streamSchema.getFields()) {
      fieldList.add(Schema.Field.of(gdatField.getName(), gdatField.getType().getSchema()));
    }
    return Schema.recordOf("GDATSchema", fieldList);
  }

  /**
   * This function invokes all the methods associated with the provided query name
   * @param queryName Name of query
   * @param decoder {@link co.cask.tigon.sql.io.GDATDecoder} object for the incoming GDAT format data record
   * @throws InvocationTargetException thrown by {@link java.lang.reflect.Method}.invoke()
   * @throws IllegalAccessException thrown by {@link java.lang.reflect.Method}.invoke()
   */
  public void invokeMethods(String queryName, GDATDecoder decoder) throws InvocationTargetException,
    IllegalAccessException {
    for (MethodInvoker methodInvoker : methodListMap.get(queryName)) {
      // Reset the position to 0 of the underlying ByteBuffer before calling MethodInvoker.invoke()
      decoder.reset();
      methodInvoker.invoke(decoder);
    }
  }
}
