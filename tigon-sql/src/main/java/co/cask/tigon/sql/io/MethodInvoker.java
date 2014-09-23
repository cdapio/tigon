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

import co.cask.tigon.internal.io.Schema;
import co.cask.tigon.internal.io.UnsupportedTypeException;
import co.cask.tigon.io.Decoder;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * MethodInvoker
 * This class is used for invoking the method of the callingObject. It uses
 * {@link co.cask.tigon.sql.io.POJOCreator} to decode the incoming data record and instantiate an object of the
 * method parameter type.
 */

public class MethodInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(MethodInvoker.class);
  private final Object callingObject;
  private final Method method;
  private final Class<?> methodParameterClass;
  private final POJOCreator pojoCreator;

  /**
   * Constructor for MethodInvoker
   * @param callingObject An instance of the Object to be used for invoking the method call
   * @param method The method object that is to be invoked by this class
   * @param inspectType {@link com.google.common.reflect.TypeToken} for the method
   * @throws UnsupportedTypeException if the {@link co.cask.tigon.sql.io.POJOCreator} cannot instantiate an
   * object of type outputClass
   * @throws java.lang.UnsupportedOperationException if the method parameter is defined through a parameterized object
   * instantiated at runtime
   */
  public MethodInvoker(Object callingObject, Method method, TypeToken<?> inspectType, Schema schema)
    throws UnsupportedTypeException {
    this.method = method;
    this.callingObject = callingObject;
    Type param = inspectType.resolveType(method.getGenericParameterTypes()[0]).getType();
    this.methodParameterClass = TypeToken.of(param).getRawType();
    if (!(param instanceof Class)) {
      LOG.error("Cannot identify type of method parameter.\tMethod : {}\tParameter : {}", method, param);
      throw new UnsupportedOperationException("Cannot identify method parameter class of parameterized objects " +
                                                "instantiated at runtime");
    }
    this.pojoCreator = new POJOCreator(methodParameterClass, schema);
  }

  /**
   * This method instantiates an object of the method parameter type using the incoming decoder object and then invokes
   * the associated method.
   * @param decoder The {@link co.cask.tigon.io.Decoder} object for the incoming data record
   * @throws InvocationTargetException thrown by {@link java.lang.reflect.Method}.invoke()
   * @throws IllegalAccessException thrown by {@link java.lang.reflect.Method}.invoke()
   */
  public void invoke(Decoder decoder) throws InvocationTargetException, IllegalAccessException {
    try {
      method.invoke(callingObject, pojoCreator.decode(decoder));
    } catch (IOException e) {
      LOG.error("Skipping invocation of method {}. Cannot instantiate parameter object of type {}",
                getMethodName(), methodParameterClass.getName(), e);
    }
  }

  /**
   * @return Method name
   */
  public String getMethodName() {
    return method.getName();
  }
}
