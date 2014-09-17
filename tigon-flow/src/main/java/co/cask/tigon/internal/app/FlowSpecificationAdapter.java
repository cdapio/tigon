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

package co.cask.tigon.internal.app;

import co.cask.tigon.api.ResourceSpecification;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.api.flow.FlowletDefinition;
import co.cask.tigon.api.flow.flowlet.FlowletSpecification;
import co.cask.tigon.internal.io.Schema;
import co.cask.tigon.internal.io.SchemaGenerator;
import co.cask.tigon.internal.io.SchemaTypeAdapter;
import co.cask.tigon.internal.io.UnsupportedTypeException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Helper class to encoded/decode {@link co.cask.tigon.api.flow.FlowSpecification} to/from json.
 */
@NotThreadSafe
public final class FlowSpecificationAdapter {

  private final SchemaGenerator schemaGenerator;
  private final Gson gson;

  public static FlowSpecificationAdapter create(SchemaGenerator generator) {
    GsonBuilder builder = new GsonBuilder();
    addTypeAdapters(builder);

    return new FlowSpecificationAdapter(generator, builder.create());
  }

  public static void addTypeAdapters(GsonBuilder builder) {
    builder
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(FlowSpecification.class, new FlowSpecificationCodec())
      .registerTypeAdapter(FlowletSpecification.class, new FlowletSpecificationCodec())
      .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
      .registerTypeAdapterFactory(new AppSpecTypeAdapterFactory());
  }

  public static FlowSpecificationAdapter create() {
    return create(null);
  }

  public String toJson(FlowSpecification appSpec) {
    try {
      StringBuilder builder = new StringBuilder();
      toJson(appSpec, builder);
      return builder.toString();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void toJson(FlowSpecification flowSpec, Appendable appendable) throws IOException {
    Preconditions.checkState(schemaGenerator != null, "No schema generator is configured. Fail to serialize to json");
    try {
      for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
        flowletDef.generateSchema(schemaGenerator);
      }
      gson.toJson(flowSpec, FlowSpecification.class, appendable);

    } catch (UnsupportedTypeException e) {
      throw new IOException(e);
    }
  }

  public void toJson(FlowSpecification appSpec, OutputSupplier<? extends Writer> outputSupplier) throws IOException {
    Writer writer = outputSupplier.getOutput();
    try {
      toJson(appSpec, writer);
    } finally {
      Closeables.closeQuietly(writer);
    }
  }

  public FlowSpecification fromJson(String json) {
    return gson.fromJson(json, FlowSpecification.class);
  }

  public FlowSpecification fromJson(Reader reader) throws IOException {
    try {
      return gson.fromJson(reader, FlowSpecification.class);
    } catch (JsonParseException e) {
      throw new IOException(e);
    }
  }

  public FlowSpecification fromJson(InputSupplier<? extends Reader> inputSupplier) throws IOException {
    Reader reader = inputSupplier.getInput();
    try {
      return fromJson(reader);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  private FlowSpecificationAdapter(SchemaGenerator schemaGenerator, Gson gson) {
    this.schemaGenerator = schemaGenerator;
    this.gson = gson;
  }

  private static final class AppSpecTypeAdapterFactory implements TypeAdapterFactory {

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      Class<?> rawType = type.getRawType();
      // note: we want ordered maps to remain ordered
      if (!Map.class.isAssignableFrom(rawType) ||
        SortedMap.class.isAssignableFrom(rawType)) {
        return null;
      }
      Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();
      TypeToken<?> keyType = TypeToken.get(typeArgs[0]);
      TypeToken<?> valueType = TypeToken.get(typeArgs[1]);
      if (keyType.getRawType() != String.class) {
        return null;
      }
      return (TypeAdapter<T>) mapAdapter(gson, valueType);
    }

    private <V> TypeAdapter<Map<String, V>> mapAdapter(Gson gson, TypeToken<V> valueType) {
      final TypeAdapter<V> valueAdapter = gson.getAdapter(valueType);

      return new TypeAdapter<Map<String, V>>() {
        @Override
        public void write(JsonWriter writer, Map<String, V> map) throws IOException {
          if (map == null) {
            writer.nullValue();
            return;
          }
          writer.beginObject();
          for (Map.Entry<String, V> entry : map.entrySet()) {
            writer.name(entry.getKey());
            valueAdapter.write(writer, entry.getValue());
          }
          writer.endObject();
        }

        @Override
        public Map<String, V> read(JsonReader reader) throws IOException {
          if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
          }
          if (reader.peek() != JsonToken.BEGIN_OBJECT) {
            return null;
          }
          Map<String, V> map = Maps.newHashMap();
          reader.beginObject();
          while (reader.peek() != JsonToken.END_OBJECT) {
            map.put(reader.nextName(), valueAdapter.read(reader));
          }
          reader.endObject();
          return map;
        }
      };
    }
  }
}
