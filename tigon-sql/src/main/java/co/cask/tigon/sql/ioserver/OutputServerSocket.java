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

import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.sql.conf.Constants;
import co.cask.tigon.sql.flowlet.GDATRecordQueue;
import co.cask.tigon.sql.flowlet.GDATRecordType;
import co.cask.tigon.sql.flowlet.StreamSchema;
import co.cask.tigon.sql.internal.StreamSchemaCodec;
import co.cask.tigon.sql.io.GDATDecoder;
import co.cask.tigon.sql.util.GDATFormatUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Output Server Socket to which Stream Engine TCP Client connects to and writes the Output Data in GDAT Format.
 */
public class OutputServerSocket extends StreamSocketServer {
  private static final Logger LOG = LoggerFactory.getLogger(OutputServerSocket.class);
  private final String outputName;
  private final CountDownLatch eofRecordLatch;
  private final ServerBootstrap dataSinkServer;
  private final GDATRecordQueue recordQueue;
  private int fieldSize = 0;
  private StreamSchema schema;
  private long dataRecordsReceived = 0;

  @SuppressWarnings("unused")
  public OutputServerSocket(ChannelFactory factory, String outputName, String sql, GDATRecordQueue recordQueue) {
    this.outputName = outputName;
    this.eofRecordLatch = new CountDownLatch(1);
    this.recordQueue = recordQueue;
    dataSinkServer = new ServerBootstrap(factory);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Output Stream {} : Starting Server", outputName);
    setDataSinkPipeline();
    Channel ch = dataSinkServer.bind(new InetSocketAddress(0));
    serverAddressMap.put(Constants.StreamIO.DATASINK, (InetSocketAddress) ch.getLocalAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info(String.format("Output Stream %s : Stopping Server", outputName));
    if (!eofRecordLatch.await(3, TimeUnit.SECONDS)) {
      LOG.warn(String.format("Output Stream %s : Didn't receive EOF Record. Proceeding with Shutdown", outputName));
    }
    dataSinkServer.shutdown();
  }

  @VisibleForTesting
  long getDataRecordsReceived() {
    return dataRecordsReceived;
  }

  private void setDataSinkPipeline() {
    setServerOptions(dataSinkServer);
    dataSinkServer.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("headerDecoder", new SchemaHeaderDecoder());
        //Length Field forms the first 4 bytes of the data record - Keep the header intact.
        pipeline.addLast("dataRecord", new LengthFieldLittleEndianFrameDecoder());
        pipeline.addLast("dataRecordHandler", new GDATRecordHandler());
        return pipeline;
      }
    });
  }

  /**
   * GDAT Records are length prefixed - [length encoded in 4bytes in BigEndian ByteOrder][data record bytes].
   * This handler waits until it gets [4 Bytes + length] bytes and passes it on to the UpstreamHandler.
   */
  private class LengthFieldLittleEndianFrameDecoder extends SimpleChannelHandler {
    private final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    private byte[] lengthBytes = new byte[Ints.BYTES];
    //TODO: Length field is unsigned 4B in GDAT Format. Check!
    private int dataRecordLength = -1;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      buffer.writeBytes((ChannelBuffer) e.getMessage());
      while (true) {
        if (dataRecordLength != -1 && buffer.readableBytes() >= dataRecordLength) {
          ChannelBuffer channelBuffer = buffer.readBytes(dataRecordLength);
          dataRecordLength = -1;
          super.messageReceived(ctx, new UpstreamMessageEvent(e.getChannel(), channelBuffer, e.getRemoteAddress()));
        } else if (dataRecordLength == -1 && buffer.readableBytes() >= Ints.BYTES) {
          //We want to pass both the length bytes and the data bytes and so using getBytes so that readerIndex
          //is not changed.
          buffer.getBytes(buffer.readerIndex(), lengthBytes, 0, Ints.BYTES);
          //Length Data is encoded as an Integer in Big Endian Format.
          //We want to wait until all bytes (ie, length integer plus data bytes arrive).
          //Hence assign dataRecordLength to length integer size plus data bytes.
          //TODO: Avoid another ByteBuffer creation if possible.
          dataRecordLength = (Ints.BYTES + ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt());
        } else {
          break;
        }
      }
    }
  }

  /**
   * Stream Engine TCP Client first sends the Schema of the Output Stream. This handler is removed after we
   * parse the Schema Header and any data records if received are passed on to UpStreamHandlers.
   */
  private class SchemaHeaderDecoder extends SimpleChannelHandler {
    private final Logger log = LoggerFactory.getLogger(SchemaHeaderDecoder.class);
    private final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    private int schemaLength = -1;

    //Find the index of Nth searchChar in a given ChannelBuffer.
    private int findNthChar(final ChannelBuffer buffer, char searchChar, int nChars) {
      int newLineCount = 0;
      final int n = buffer.writerIndex();
      for (int i = buffer.readerIndex(); i < n; i++) {
        final byte b = buffer.getByte(i);
        if (b == searchChar) {
          newLineCount++;
          if (newLineCount == nChars) {
            return i;
          }
        }
      }
      return -1;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      buffer.writeBytes((ChannelBuffer) e.getMessage());
      log.info("Output Stream {} : Got some data {}", outputName, buffer.readableBytes());
      while (true) {
        //GDAT\nVERSION:4\nSCHEMALENGTH:123\n - Find the third newline byte position.
        if (schemaLength == -1 && (findNthChar(buffer, '\n', 3) != -1)) {
          final int newLinePos = findNthChar(buffer, '\n', 3);
          //VERSION:4\nSCHEMALENGTH:123\n present
          final int colonPos = findNthChar(buffer, ':', 2);
          int byteArrayLength = (newLinePos - colonPos) - 1;
          ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
          buffer.getBytes(colonPos + 1, byteOutput, byteArrayLength);
          //schemalength in the header does not include the length of the header. so add it to num bytes expected.
          schemaLength = newLinePos + 1 + Integer.valueOf(Bytes.toString(byteOutput.toByteArray()));
        } else if (schemaLength != -1 && buffer.readableBytes() >= schemaLength) {
          byte[] schemaBytes = buffer.readBytes(schemaLength).array();
          String schemaString = GDATFormatUtil.getSerializedSchema(Bytes.toString(schemaBytes));
          schema = StreamSchemaCodec.deserialize(schemaString);
          fieldSize = GDATFormatUtil.getGDATFieldSize(schema);
          //Remove this handler since we have received the schema string.
          ctx.getPipeline().remove("headerDecoder");
          LOG.info("Output Stream {} : Received Schema!", outputName);
          //Send any remaining bytes in ChannelBuffer upstream.
          if (buffer.readableBytes() != 0) {
            ChannelBuffer newBuffer = buffer.readBytes(buffer.readableBytes());
            super.messageReceived(ctx, new UpstreamMessageEvent(e.getChannel(), newBuffer, e.getRemoteAddress()));
            break;
          }
        } else {
          break;
        }
      }
    }
  }

  /**
   * Handles GDAT Records.
   */
  private class GDATRecordHandler extends SimpleChannelHandler {
    private final Logger log = LoggerFactory.getLogger(GDATRecordHandler.class);

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
      if (GDATRecordType.getGDATRecordType(buffer.getByte(fieldSize + Ints.BYTES)).equals(GDATRecordType.EOF)) {
        eofRecordLatch.countDown();
        log.info(String.format("Output Stream %s : Received EOF Record", outputName));
        //TODO: Let Health Manager know (and let it decide if it is an failure case)?
      } else {
        GDATDecoder decoder = new GDATDecoder(buffer.toByteBuffer());
        //Add Decoder to queue
        recordQueue.add(Maps.immutableEntry(outputName, decoder));
        dataRecordsReceived++;
        super.messageReceived(ctx, e);
      }
    }
  }
}
