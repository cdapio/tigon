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

package co.cask.tigon.sql.ioserver;

import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.util.GDATFormatUtil;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Json Input Format Server Socket - Converts data in JSON format to GDAT format.
 * JSON string is expected to be in the following format :
 * {"data" : ["123", "Foo", "True", "34.5"]}
 */
public class JsonInputServerSocket extends InputServerSocket {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, List<String>>>() { }.getType();
  private final StreamSchema schema;

  public JsonInputServerSocket(ChannelFactory factory, String name, StreamSchema inputSchema) {
    super(factory, name, inputSchema);
    this.schema = inputSchema;
  }

  @Override
  public LinkedHashMap<String, ChannelHandler> addTransformHandler() {
    LinkedHashMap<String, ChannelHandler> handlers = Maps.newLinkedHashMap();
    handlers.put("jsonMapDelimiter", new DelimiterBasedFrameDecoder(Integer.MAX_VALUE, false,
                                                                    ChannelBuffers.wrappedBuffer(new byte[] {'}'})));
    handlers.put("stringDecoder", new StringDecoder());
    handlers.put("jsonHandler", new JsonHandler(schema));
    return handlers;
  }

  @SuppressWarnings("unchecked")
  private static class JsonHandler extends SimpleChannelHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JsonHandler.class);
    private final StreamSchema schema;
    private final ByteArrayOutputStream outputStream;

    public JsonHandler(StreamSchema schema) {
      this.schema = schema;
      this.outputStream = new ByteArrayOutputStream();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      String json = (String) e.getMessage();
      Map<String, List<String>> jsonMap = GSON.fromJson(json, MAP_TYPE);
      GDATFormatUtil.encode(jsonMap.get("data"), schema, outputStream, false);
      //TODO: Use OutStream in GDATEncoder to avoid making toByteArray call since it makes a copy.
      ChannelBuffer dataRecordBuffer = ChannelBuffers.copiedBuffer(outputStream.toByteArray());
      outputStream.reset();
      MessageEvent newMessageEvent = new UpstreamMessageEvent(ctx.getChannel(), dataRecordBuffer, e.getRemoteAddress());
      super.messageReceived(ctx, newMessageEvent);
    }
  }
}
